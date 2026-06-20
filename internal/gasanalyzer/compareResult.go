package gasanalyzer

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/ethclient"
)

func (a *Analyzer) CompareFeeHistory(client *ethclient.Client) {
	a.mu.Lock()
	preResult := a.latestResult
	a.mu.Unlock()

	ctx := context.Background()
	if preResult.NextBlockNumber == 0 || preResult.analyzerBlock == nil {
		logger.Info(ctx, "empty result data",
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	per := make([]float64, 0, len(GasPredictionTargets))

	for _, t := range GasPredictionTargets {
		per = append(per, t.Ratio*100)
	}

	history, err := client.FeeHistory(ctx, 1, big.NewInt(int64(preResult.NextBlockNumber)), per)
	if err != nil {
		logger.Error(ctx, "failed to get block fee history",
			err,
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//결과 비교
	if len(history.Reward) > 0 && len(history.BaseFee) >= 2 {
		reward := history.Reward[0]

		for i, t := range GasPredictionTargets {

			actualTip := reward[i].Uint64()

			if _, ok := preResult.analyzerBlock[t.Name]; ok {
				anaBlock := int64(preResult.analyzerBlock[t.Name].PriorityFee)
				anaPending := int64(preResult.analyzerPending[t.Name].PriorityFee)
				oraBlock := int64(preResult.oracleBlock[t.Name].PriorityFee)
				oraPending := int64(preResult.oraclePending[t.Name].PriorityFee)

				blend := calculateFourWayBlend(t.Name, anaBlock, anaPending, oraBlock, oraPending)
				diff := blend - int64(actualTip)

				// 1. 모든 가스비 지표를 콤마가 포함된 문자열로 변환
				// diff의 경우 음수 기호(-)를 살리기 위해 int64 포맷팅을 유지합니다.
				sAnaBlock := humanize.Comma(anaBlock)
				sAnaPending := humanize.Comma(anaPending)
				sOraBlock := humanize.Comma(oraBlock)
				sOraPending := humanize.Comma(oraPending)
				sBlend := humanize.Comma(blend)
				sActual := humanize.Comma(int64(actualTip))
				sDiff := humanize.Comma(diff)
				if diff > 0 {
					sDiff = "+" + sDiff // 양수일 때 + 기호 추가로 시인성 확보
				}

				// 2. 포맷터를 %d에서 %s(문자열)로 변경하여 출력
				fmt.Printf(
					"%-10s | %-14s | %-14s | %-14s | %-14s | %-14s | %-14s | %-12s\n",
					t.Name,
					sAnaBlock,
					sAnaPending,
					sOraBlock,
					sOraPending,
					sBlend,
					sActual,
					sDiff,
				)

			} else {
				fmt.Printf(
					"%-10s | 데이터 없음\n", t.Name)
			}
		}

		fmt.Printf("BaseFee - 예측 : %d 실제 : %d \n", preResult.NextBaseFee, history.BaseFee[0].Uint64())
	}
}

func calculateFourWayBlend(levelName string, anaBlock, anaPending, oraBlock, oraPending int64) int64 {
	// 예외 처리: 데이터가 비정상적일 때의 최소한의 방어선
	if anaPending <= 0 {
		return (anaBlock + oraBlock) / 2
	}

	maxVal := math.Max(float64(anaPending), float64(anaBlock))
	minVal := math.Min(float64(anaPending), float64(anaBlock))

	// 두 값의 차이가 전혀 없으면 0.0, 한쪽이 압도적으로 크면 1.0에 수렴하는 계수
	gapRatio := (maxVal - minVal) / maxVal

	var wAnaPending float64 // 실시간 밈풀(단기 감쇠) 가중치
	var wAnaBlock float64   // 최근 블록 트렌드(단기 감쇠) 가중치
	var wOraBlock float64   // 장기 블록 분포(히스토그램) 가중치
	var wOraPending float64 // 실시간 밈풀 분포(히스토그램) 가중치

	// -------------------------------------------------------------------------
	// 📊 [가중치 분배] 가스 레벨별 4대 지표 융합 밸런싱
	// -------------------------------------------------------------------------
	switch levelName {
	case "low":
		// Low는 무조건 비용 절감이 목적이므로 밈풀 양대 지표에 95% 몰아줍니다.
		wAnaPending = 0.75 + (gapRatio * 0.15) // 격차가 클수록 최신 밈풀 신뢰 (최대 0.90)
		wOraPending = 0.20 - (gapRatio * 0.10) // 격차가 작을 때 백업 (최소 0.10)
		wAnaBlock = 1.0 - (wAnaPending + wOraPending)
		wOraBlock = 0.0

	case "market":
		// Market은 대다수의 트랜잭션 기준점이므로 안정적인 밸런스를 잡되,
		// 격차가 커질수록 최신 밈풀(anaPending) 비중을 최대 85%까지 확대합니다.
		wAnaPending = 0.65 + (gapRatio * 0.20)
		wOraPending = 0.15
		wAnaBlock = 1.0 - (wAnaPending + wOraPending)
		wOraBlock = 0.0

	case "fast":
		// Fast는 다음 1~2블록 내 처리가 목표이므로 블록 데이터도 중요합니다.
		// 평시에는 anaPending 50%, 블록 데이터 합쳐서 40%, oraPending 10% 비율로 섞고
		// 격차가 커지면 실시간 상황(anaPending) 위주로 전형을 전환합니다.
		wAnaPending = 0.50 + (gapRatio * 0.25) // 격차 발생 시 최대 0.75까지 상승

		remaining := 1.0 - wAnaPending
		wAnaBlock = remaining * 0.60   // 남은 지분의 60%는 최신 블록 트렌드
		wOraBlock = remaining * 0.25   // 남은 지분의 25%는 장기 블록 트렌드
		wOraPending = remaining * 0.15 // 남은 지분의 15%는 밈풀 히스토그램

	case "urgent":
		// Urgent는 안전제일 최우선 구간입니다.
		// 평시에는 네 지표를 골고루 섞어(45 : 35 : 15 : 5) 평균 안정성을 유지하다가
		// 변동성 격차가 커지면 anaPending 비중을 75%까지 끌어올려 실시간 대응을 합니다.
		wAnaPending = 0.45 + (gapRatio * 0.30)

		remaining := 1.0 - wAnaPending
		wAnaBlock = remaining * 0.65
		wOraBlock = remaining * 0.25
		wOraPending = remaining * 0.10

	default:
		wAnaPending = 0.50
		wAnaBlock = 0.25
		wOraBlock = 0.15
		wOraPending = 0.10
	}

	// -------------------------------------------------------------------------
	// 🧮 [최종 연산] 4대 가중치 결합 및 안전장치(Clamping)
	// -------------------------------------------------------------------------
	blend := (float64(anaPending) * wAnaPending) +
		(float64(anaBlock) * wAnaBlock) +
		(float64(oraBlock) * wOraBlock) +
		(float64(oraPending) * wOraPending)

	// 극단적인 수치 튐을 방지하기 위한 세이프 가드 (Clamping)
	// 단, 폭락장 레이어가 가동되지 않는 일반 상황에서만 상하한선을 제어합니다.
	absoluteMax := math.Max(float64(anaPending), math.Max(float64(anaBlock), float64(oraBlock)))
	absoluteMin := math.Min(float64(anaPending), math.Min(float64(anaBlock), float64(oraBlock)))

	allowedMin := absoluteMin * 0.9
	allowedMax := absoluteMax * 1.1

	if blend < allowedMin {
		blend = allowedMin
	} else if blend > allowedMax {
		blend = allowedMax
	}

	return int64(math.Round(blend))
}
