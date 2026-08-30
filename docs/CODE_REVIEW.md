# Code Review — eth-mon-svr

> 종합 코드 리뷰. 코드를 수정하지 않고 분석 결과만 기록한다.
> 심각도: `CRITICAL` > `HIGH` > `MEDIUM` > `LOW`
> 확실한 버그(확정)와 가능성 수준(잠재)을 구분해서 표기한다.

---

## 요약

발견한 문제 중 운영 장애로 직결될 수 있는 핵심 항목:

| # | 심각도 | 위치 | 요약 |
|---|---|---|---|
| 1 | HIGH | `cmd/server/main.go` | 에러 발생 시 nil 의존성 그대로 진행 → 후속 panic |
| 2 | HIGH | `ingestion/header.go` `consumeHeaderStream` | watchdog timer가 재설정되지 않아 header timeout이 영구 미동작 |
| 3 | HIGH | `coordinator/blockPipeline.go` `checkBlockGap` | 백필에서 nil header를 저장 → `checkStale` nil deref panic |
| 4 | HIGH | `ingestion/pending.go` `runPendingSub` | 최초 pending 세션 실패 시 재시도 없이 영구 종료 |
| 5 | MEDIUM | `processor/processor.go` `CompareFeeHistory` | `reward[i]` index out of range 가능 |
| 6 | MEDIUM | `grpcClient/client.go` `sendFeeBucket` | 서버 응답 `Success=false`를 무시 |
| 7 | MEDIUM | `ingestion/pending.go` `runPendingSub` | `s.providers[0]` index out of range |
| 8 | MEDIUM | `grpcClient/client.go` `enqueue` | 버퍼 초과 시 예측/갭 메시지 조용히 유실 |
| 9 | MEDIUM | `coordinator/pendingPipeline.go` `push`/`stop` | close된 채널에 send → panic 가능 |

---

## 상세 분석

### 1. main.go — 에러 발생 시 nil 의존성 그대로 진행

- **[Severity]** HIGH (확정)
- **[Location]** `cmd/server/main.go` `main()` — `NewAuthGrpcClient`/`NewTokenManager`/`NewGasPredictClient` 에러 분기
- **[Problem]**
  ```go
  authGrpcClient, err := auth.NewAuthGrpcClient(cfg.GrpcServerAddr)
  if err != nil {
      logger.Error(ctx, "fail to connet auth gRPC ", err, ...)
      // return/panic 없이 계속 진행
  }
  defer authGrpcClient.Close()

  tokenManager, err := auth.NewTokenManager(authGrpcClient, cfg.Service, cfg.AuthClientSecret)
  if err != nil {
      logger.Error(ctx, "fail to generate token manager", err, ...)
      // 계속 진행 -> tokenManager == nil
  }

  grpcClient, cleanup, err := grpcClient.NewGasPredictClient(ctx, cfg.GrpcServerAddr, tokenManager)
  if err != nil {
      logger.Error(ctx, "fail to connet gRPC ", err, ...)
      // 계속 진행 -> grpcClient == nil, cleanup == nil
  }
  defer cleanup()
  ```
- **[Why]** 세 곳 모두 에러를 로그만 남기고 계속 진행한다. 실패 시 각 변수가 `nil`이 되어 이후 사용처에서 panic이 발생한다.
- **[Scenario]**
  1. auth 서버 접속 실패 → `authGrpcClient == nil` → `NewTokenManager(nil, ...)`가 `"auth grpc client is nil"` 반환 → `tokenManager == nil`
  2. `NewGasPredictClient(..., nil)` → `auth.UnaryClientInterceptor(nil)`/`StreamClientInterceptor(nil)`가 nil `tokenManager`를 캡처한 인터셉터 생성
  3. 첫 gRPC 호출 시 인터셉터가 `tokenManager.GetAccessToken(ctx)` 호출 → **nil pointer dereference panic**
  4. `NewGasPredictClient`가 실패하면 `cleanup == nil` → `defer cleanup()`에서 **nil 함수 호출 panic**
- **[Impact]** auth/gRPC 연결 실패 시 프로세스가 즉시 panic하거나, 뒤늦게 첫 RPC 시점에 panic. graceful shutdown/재시도 불가.
- **[Recommendation]** 각 단계에서 실패 시 즉시 `return`(또는 `panic`)으로 종료하거나, nil 체크 후 진행. 특히 `cleanup`은 nil 가드 후 defer.

---

### 2. header.go — watchdog timer 미재설정으로 header timeout 영구 미동작

- **[Severity]** HIGH (확정)
- **[Location]** `ingestion/header.go` `consumeHeaderStream` (watchdog 블록)
- **[Problem]**
  ```go
  watchdog := time.NewTimer(watchdogInterval) // 10초, 1회성
  for {
      select {
      case <-watchdog.C:
          if time.Since(lastHeaderAt) >= headerTimeout { // 30초
              return errHeaderTimeout...
          }
          // <- timer.Reset() 없음!
      }
  }
  ```
- **[Why]** `time.NewTimer`는 1회만 발화한다. 첫 발화(t=10s) 시점에 `time.Since(lastHeaderAt)`는 기껏해야 10초(최초 `lastHeaderAt=time.Now()`)라 `>= headerTimeout(30s)` 조건이 항상 false → timeout 반환 없음. 이후 타이머는 소진되어 더 이상 발화하지 않으므로 **header timeout 검사가 영구적으로 수행되지 않는다.**
- **[Scenario]** 헤더 구독 WS가 에러 없이 조용히 멈춘 경우(예: 중간 장비에서 연결이 hang), `sub.Err()`도 안 오고 헤더도 안 오는 상태가 되어도 30초 stale 감지가 동작하지 않아 재연결이 일어나지 않는다.
- **[Impact]** 헤더 수신 중단을 감지하지 못해 블록 처리·가스 분석이 멈춘 상태로 방치된다. (헤더 stale 감지 기능이 사실상 무력화)
- **[Recommendation]** 발화 후 `watchdog.Reset(watchdogInterval)` 호출하거나 `time.NewTicker` + 매 틱 `time.Since(lastHeaderAt)` 비교로 변경.

---

### 3. coordinator — 백필에서 nil header 저장 → checkStale nil deref

- **[Severity]** HIGH (잠재)
- **[Location]** `coordinator/blockPipeline.go` `checkBlockGap` / `procAvailable` / `checkStale`
- **[Problem]**
  ```go
  // checkBlockGap (백필)
  header, err := b.proc.HeaderByNumber(ctx, num)
  if err != nil {
      return
  }
  b.pendingBlock[num] = header   // header가 nil일 수 있음 (블록 미존재)
  ...
  // procAvailable
  err := b.procWithRetry(ctx, header)   // nil header -> ProcessBlock -> ErrLatestBlockHeaderNil
  if err != nil {
      latest, stale, e := b.checkStale(ctx, header)  // <- nil header 전달
  }
  // checkStale
  latest, err := b.proc.HeaderByNumber(ctx, header.Number.Uint64()) // header.Number nil -> panic
  ```
- **[Why]** `processor.HeaderByNumber`는 블록이 존재하지 않으면 `(nil, nil)`을 반환한다. `checkBlockGap`의 백필은 nil 체크 없이 `pendingBlock[num]`에 저장하고, 이후 `procAvailable` → `checkStale`에서 `header.Number.Uint64()`를 호출해 **nil pointer dereference panic**이 발생할 수 있다.
- **[Scenario]** gap 백필 대상 블록이 체인에 실제로 존재하지 않는 경우(리오그, 아직 미생성된 블록 등). `HeaderByNumber`가 nil을 반환하면 프로세스 panic.
- **[Impact]** coordinator goroutine panic → 블록 파이프라인 정지(프로세스 전체는 recovery 없이 죽을 수 있음).
- **[Recommendation]** 백필 후 `header == nil || header.Number == nil`이면 해당 블록을 건너뛰거나(`continue`) resync로 전환. `checkStale`/`procWithRetry` 호출 전 nil 가드 추가.

---

### 4. pending.go — 최초 pending 세션 실패 시 재시도 없이 영구 종료

- **[Severity]** HIGH (확정)
- **[Location]** `ingestion/pending.go` `runPendingSub`
- **[Problem]**
  ```go
  curProvider := s.providers[0]
  session, err := s.startPendingSession(ctx, curProvider)
  if err != nil {
      logger.Error(ctx, "initial pending session failed", err, ...)
      return // <- 재시도 없이 종료
  }
  // 이후 for-select loop (재연결/핸드오버)는 "최초 성공" 후에만 동작
  ```
- **[Why]** header 구독(`runHeaderSub`)은 재연결 루프를 갖지만, pending 구독은 최초 세션 연결 실패 시 즉시 `return`하여 영구적으로 종료된다. 이후 재시도/재연결 로직이 없다.
- **[Scenario]** 서비스 기동 시 pending WS provider가 일시적으로 다운된 경우, 해당 프로세스는 pending 트랜잭션을 영구히 수신하지 못한다(헤더는 재시도하지만 pending은 죽음).
- **[Impact]** pending tx 수집 불능 → 가스 예측의 pending 성분 상실. 프로세스는 "정상 동작"으로 보이나 실제로는 일부 기능만 동작.
- **[Recommendation]** 최초 실패 시에도 재연결 루프(`for` + `WaitForRetry`)에 진입하도록 구조 변경.

---

### 5. CompareFeeHistory — `reward[i]` index out of range

- **[Severity]** MEDIUM (잠재)
- **[Location]** `processor/processor.go` `CompareFeeHistory`
- **[Problem]**
  ```go
  if len(history.Reward) > 0 && len(history.BaseFee) >= 2 {
      reward := history.Reward[0]
      for i, t := range gasanalyzer.GasPredictionTargets { // 3개
          actualTip := reward[i].Uint64() // reward 길이가 3 미만이면 panic
      }
  }
  ```
- **[Why]** `len(history.Reward) > 0`은 블록이 1개 이상인지만 확인한다. `history.Reward[0]`(보상 배열)의 길이가 `len(GasPredictionTargets)`(3)과 일치하는지 확인하지 않아, RPC 응답이 예상보다 적은 보상 항목을 반환하면 `reward[i]`에서 **index out of range panic**.
- **[Scenario]** RPC 노드가 요청한 percentile 수만큼 보상을 반환하지 않는 경우(구현 차이, 비정상 응답).
- **[Impact]** 프로세스 panic.
- **[Recommendation]** `len(reward) >= len(GasPredictionTargets)` 확인 후 순회.

---

### 6. sendFeeBucket — 서버 `Success=false` 응답 무시

- **[Severity]** MEDIUM (확정)
- **[Location]** `grpcClient/client.go` `sendFeeBucket`
- **[Problem]**
  ```go
  _, err := c.client.UploadFeeBuckets(ctx, req)
  if err == nil {
      return nil // <- resp.Success 확인 없음
  }
  ```
  반면 gas prediction 스트림 쪽(`processStream`)은 `res.Success`를 확인한다.
- **[Why]** 서버(eth-web-svr)는 비즈니스 실패를 gRPC 에러가 아닌 `CommonResponse{Success:false}`로 반환한다. 이를 무시하면 서버가 거부한 fee bucket을 "전송 성공"으로 간주한다.
- **[Scenario]** 서버가 fee bucket을 검증/거부(`Success:false`)해도 클라이언트는 재시도하지 않고 정상 처리로 간주.
- **[Impact]** fee 통계 데이터 유실(조용히 무시).
- **[Recommendation]** `resp.Success` 확인, `false`면 에러로 처리(재시도).

---

### 7. runPendingSub — `s.providers[0]` index out of range

- **[Severity]** MEDIUM (잠재)
- **[Location]** `ingestion/pending.go` `runPendingSub`
- **[Problem]**
  ```go
  curProvider := s.providers[0] // providers가 비어 있으면 panic
  ```
- **[Why]** `NewSubscriber`가 providers 비어있음을 검증하지 않는다. config가 WS provider를 채워주므로 현재는 발생하지 않지만, 방어적으로는 빈 슬라이스에서 panic 위험이 있다.
- **[Impact]** 빈 provider 설정 시 프로세스 panic.
- **[Recommendation]** `len(s.providers) == 0` 검사 추가(또는 `NewSubscriber`에서 검증).

---

### 8. enqueue — 채널 가득 시 예측/갭 메시지 조용히 유실

- **[Severity]** MEDIUM (확정)
- **[Location]** `grpcClient/client.go` `enqueue` / `GasPredictResultSend`
- **[Problem]**
  ```go
  func (c *GasPredictionClient) enqueue(req *pb.GasPredictionStream) {
      select {
      case c.GasPredictCh <- req:
      default:
          logger.Warn(..., "send channel full and GasPredictionRequest dropp the data")
      }
  }
  ```
- **[Why]** `GasPredictCh`(버퍼 300)가 가득 차면 예측 메시지와 갭 메시지가 **조용히 drop**된다. gRPC 스트림이 끊기거나 느린 동안 예측 데이터가 유실된다.
- **[Scenario]** gRPC 서버 다운/스트림 재연결 지연으로 `GasPredictCh`가 쌓여 가득 찬 경우, 이후 예측 결과(및 연결 갭 표시)가 유실.
- **[Impact]** 프론트엔드로 전달되는 가스 예측/갭 정보 누락.
- **[Recommendation]** drop 정책이 의도라면 명시적으로 유지하되, 갭(gap) 메시지는 유실 시 재생성 로직이 있으므로 예측 drop 빈도를 메트릭으로 노출하는 것이 좋다.

---

### 9. pendingPipeline — `stop` 이후 `push` 시 close된 채널에 send → panic

- **[Severity]** MEDIUM (잠재)
- **[Location]** `coordinator/pendingPipeline.go` `stop` / `push`
- **[Problem]**
  ```go
  func (p *pendingPipeline) stop() { close(p.jobs); p.wg.Wait() }

  func (p *pendingPipeline) push(hash common.Hash) {
      select {
      case p.jobs <- hash: // jobs가 이미 close면 panic
      default:
      }
  }
  ```
- **[Why]** `stop()`이 `jobs`를 close한 뒤에도 `push`가 호출되면 close된 채널에 send하여 panic 발생. 현재는 `main.go`에서 `sub.Wait()`(→push 중단) 후 `coor.Stop()`(→close) 순서라 안전하지만, 순서가 바뀌거나 이벤트가 늦게 도착하면 위험.
- **[Scenario]** shutdown 경쟁 상황에서 ingestion이 `PushTxHash`를 호출하는 중 `coor.Stop()`이 실행되는 경우.
- **[Impact]** 프로세스 panic.
- **[Recommendation]** close 책임과 send 책임을 분리하거나, `push`에서 recover/ok 체크. `stop`에서 close 전에 더 이상 push가 없음을 보장.

---

### 10. NewRpcManager — nil client에 Close 호출 (데드 코드)

- **[Severity]** LOW
- **[Location]** `rpcManager/manager.go` `NewRpcManager`
- **[Problem]**
  ```go
  client, err := NewEthClient(provider, url, policy)
  if err != nil {
      client.Close() // NewEthClient는 에러 시 nil을 반환
      continue
  }
  ```
- **[Why]** `NewEthClient`는 dial 실패 시 `(nil, err)`을 반환하므로 `client.Close()`는 항상 `nil.Close()`(nil 가드로 no-op). 의도(실패 시 정리)와 다른 데드 코드.
- **[Impact]** 없음(안전). 다만 이후 리팩토링 시 오해 소지.
- **[Recommendation]** 제거 또는 주석 명확화.

---

### 11. DecayValues — 내부 슬라이스 뷰 노출

- **[Severity]** LOW
- **[Location]** `gasanalyzer/fee.go` `DecayValues`
- **[Problem]**
  ```go
  func (a *Analyzer) DecayValues() []float64 { return a.DecayTable[:] }
  ```
- **[Why]** 내부 `DecayTable` 배열의 슬라이스 뷰를 그대로 반환해 호출자가 내부 상태를 변경할 수 있다. 현재 호출처(processor)는 읽기만 하므로 실질적 위험은 낮다.
- **[Impact]** 의도치 않은 상태 변경 가능성(잠재).
- **[Recommendation]** 복사본 반환 또는 read-only 보장.

---

### 12. handoverPending — 1초 타임아웃으로 old 세션 goroutine 누수 가능

- **[Severity]** LOW
- **[Location]** `ingestion/pending.go` `handoverPending`
- **[Problem]**
  ```go
  old.cancel()
  select {
  case <-old.done:
  case <-time.After(1 * time.Second):
  }
  ```
- **[Why]** `old.cancel()` 후 `old.done`을 1초 대기. `connectPendingAndStream`이 gRPC 송수신에 블로킹되어 1초 내 반환하지 못하면 old 세션 goroutine이 남는다(누수). 이후 ctx 취소로 종료되긴 하지만 지연.
- **[Impact]** 짧은 goroutine 잔류(리소스 누수).
- **[Recommendation]** `old.done` 대기 로직을 ctx 연동으로 개선하거나, 세션 종료를 보장하는 별도 메커니즘 사용.

---

### 13. collectPendingTx — `cutoff` 파라미터 미사용

- **[Severity]** LOW
- **[Location]** `gasanalyzer/analyzer.go` `collectPendingTx`
- **[Problem]** `cutoff` 인자를 받지만 본문에서 사용하지 않는다(하위 20% cutoff 로직이 주석 처리됨).
- **[Why]** 호출부는 `cutoff`를 계산해 전달하지만 실제 필터링에는 반영되지 않는다.
- **[Impact]** 기능 미동작(의도가 있다면 누락).
- **[Recommendation]** 사용하거나 파라미터 제거.

---

### 14. UpdateBlockInfoForAnalysis — DecayTable 크기가 블록 수를 제한

- **[Severity]** LOW
- **[Location]** `processor/processor_block.go` `UpdateBlockInfoForAnalysis`
- **[Problem]**
  ```go
  decayTable := p.gasanalyzer.DecayValues() // 길이 21
  for i, b := range blockData {
      if i >= len(decayTable) { break }
      ...
  }
  ```
- **[Why]** `DecayTable` 길이(MaxAge+1=21)가 블록 분석 대상 수를 암묵적으로 제한한다. `MaxBlockCount`가 21보다 크게 설정되면 그 이후 블록은 분석에서 조용히 제외된다.
- **[Impact]** 블록 수를 늘려도 분석 반영 블록이 21개로 제한(설정 의도와 불일치 가능).
- **[Recommendation]** 두 값의 관계를 명시하거나, decay를 블록 수에 맞게 확장.

---

### 15. config — `MaxBlockCount=0` 시 blockstore 무효화 + 상시 resync

- **[Severity]** LOW
- **[Location]** `config/config.go` / `blockstore/store.go` / `coordinator/blockPipeline.go`
- **[Problem]** `MAX_BLOCK_COUNT`가 0이면 `NewBlockStore(0)`은 블록을 즉시 truncate하고, `blockPipeline.maxBlockLag=0`이 되어 `checkBlockGap`에서 `gap >= 0`이 항상 참 → 사소한 gap에도 resync.
- **[Impact]** 잘못된 설정 시 블록 히스토리 미저장 + 잦은 resync.
- **[Recommendation]** config 검증(최소값) 추가.

---

### 16. auth TokenManager — RPC 수행 중 mutex 보유

- **[Severity]** LOW
- **[Location]** `network/auth/tokenManager.go` `GetAccessToken`
- **[Problem]** `GetAccessToken`이 `m.mu.Lock()`을 잡고 `authenticateLocked`(gRPC 호출)을 수행한다.
- **[Why]** 토큰 갱신 중 다른 호출자 전부 블로킹. 정합성은 유지되나 네트워크 지연 시 지연 전파.
- **[Impact]** 동시 요청 시 지연.
- **[Recommendation]** singleflight/조건변수 패턴으로 개선(성능 최적화, 필수 아님).

---

## 카테고리별 추가 관찰 (낮은 우선순위)

- **로그만 남기고 처리하지 않음**: `main.go`의 auth/gRPC 연결 실패(#1), `processor`의 여러 RPC 실패 경로는 로그 후 `continue`/`return`으로 해당 데이터를 조용히 유실(개별 tx/영수증 조회 실패). 의도된 graceful degradation이나, 유실 카운터가 없어 관측이 어렵다.
- **`Process.mu` 미사용**: `processor/processor.go`의 `Process.mu`는 현재 어느 메서드에서도 사용되지 않는 데드 필드.
- **`connectAndStream`/`consumeHeaderStream`의 `ch` 수신 시 `ok` 미검사**: 구독 채널이 닫히면 zero value를 반복 수신. `sub.Err()`가 함께 닫히므로 실질적 무한 루프는 아니지만, 명시적 `ok` 체크가 안전.
- **`processStream`의 채널 close 분기**: `GasPredictCh`는 프로그램 어디에서도 close되지 않아 `if !ok { return nil }`은 데드 코드.
- **`analyzer.Start(ctx)`가 main goroutine을 블로킹**: shutdown 대기 역할을 겸하고 있어 `<-ctx.Done()`이 뒤에 중복 존재. 동작은 정상이나 의미가 모호.

---

## 관찰 범위 밖 (의도된 것으로 보이는 것)

- `rpcmanager.EthClientFunc`의 failover/rotation 로직: mutex로 보호되고, rate limit 초과·nil client skip 처리가 되어 있음(이전 수정 반영됨).
- `mempool.Snapshot`: RLock + 값 복사로 안전. `nonces` 빈 경우 가드 존재.
- `coordinator.blockPipeline` 상태(`pendingBlock`/`nextBlockNum`)는 단일 goroutine(`blockProcLoop`)에서만 접근되어 data race 없음.
- `ingestion` provider rotation(`provider`/`alternateProvider`): empty URL·자기 자신 skip 등 처리됨.
