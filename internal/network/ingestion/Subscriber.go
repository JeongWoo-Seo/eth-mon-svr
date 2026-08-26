package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/coordinator"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/rpc"
)

type Subscriber struct {
	coor  *coordinator.Coordinator
	dedup *Cache
	wg    sync.WaitGroup

	providers     []Provider
	pendingSwitch chan string

	//Subscriber test를 위해 외부 네트워크 연결 func을 struct에 포함함
	connectPendingStream func(ctx context.Context, session *pendingSession) error
}

func NewSubscriber(providers []Provider, coor *coordinator.Coordinator) (*Subscriber, error) {
	dedup, err := NewCache(txDedupCasheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create dedup cashe: %w", err)
	}

	s := &Subscriber{
		coor:          coor,
		dedup:         dedup,
		providers:     providers,
		pendingSwitch: make(chan string, 4),
	}
	s.connectPendingStream = s.connectPendingAndStream

	return s, nil
}

func (s *Subscriber) SubscriberStart(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runHeaderSub(ctx)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runPendingSub(ctx)
	}()
}

func (s *Subscriber) Wait() {
	s.wg.Wait()
}

func (s *Subscriber) notifyPendingSwitch(provider string) {
	select {
	case s.pendingSwitch <- provider:
	default:
	}
}

func (s *Subscriber) provider(name string) (Provider, bool) {
	for _, p := range s.providers {
		if p.Name == name {
			if p.Url == "" {
				return Provider{}, false
			}
			return p, true
		}
	}
	return Provider{}, false
}

func (s *Subscriber) alternateProvider(name string) (Provider, bool) {
	providers := s.providers
	n := len(providers)
	if n == 0 {
		return Provider{}, false
	}

	//시작 idx 찾기
	curIdx := -1
	for i, p := range s.providers {
		if p.Name == name {
			curIdx = i
			break
		}
	}

	if curIdx == -1 {
		curIdx = 0
	}

	// 시작 idx부터 provider loop
	for i := 1; i <= n; i++ {
		nextIdx := (curIdx + i) % n
		p := providers[nextIdx]

		if p.Name == name || p.Url == "" {
			continue
		}
		return p, true
	}
	return Provider{}, false
}

func subscription[T any](
	ctx context.Context,
	url string,
	label string,
	method string,
	buff int,
	outCh chan<- T,
	dedup *Cache,
) {
	for {
		err := connectAndStream(ctx, url, label, method, buff, outCh, dedup)
		if err != nil {
			logger.Error(ctx, "ethereum disconnected",
				err,
				slog.String("system", "ethereum"),
				slog.String("action", "disconnect"),
				slog.String("subscription", label),
			)
		}

		select {
		case <-ctx.Done():
			logger.Info(ctx, "ethereum subscription stopped",
				slog.String("system", "ethereum"),
				slog.String("action", "shutdown"),
				slog.String("subscription", label),
			)
			return
		case <-time.After(reconnectDelay):
			logger.Info(ctx, "ethereum reconnect attempt",
				slog.String("system", "ethereum"),
				slog.String("action", "reconnect"),
				slog.String("subscription", label),
				slog.Duration("delay", reconnectDelay),
			)
		}
	}
}

func connectAndStream[T any](
	ctx context.Context,
	url string,
	label string,
	method string,
	buff int,
	outCh chan<- T,
	dedup *Cache,
) error {
	client, err := rpc.DialContext(ctx, url)
	if err != nil {
		return err
	}
	defer client.Close()

	ch := make(chan T, buff)
	sub, err := client.EthSubscribe(ctx, ch, method)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	logger.Info(ctx, "ethereum subscribe",
		slog.String("system", "ethereum"),
		slog.String("action", "subscribe"),
		slog.String("subscription", label),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case data := <-ch:
			if label == "PendingTx" {
				report.IncPendginRecieved()
				if txHash, ok := any(data).(string); ok {
					if dedup != nil && dedup.Seen(txHash) {
						continue // 이미 본 트랜잭션은 채널에 넣지도 않고 무시
					}
				}
			}

			select {
			case outCh <- data:
			default:
				//pool이 꽉 찼을 때만 Drop
			}
		}
	}
}
