package ingestion

import (
	"context"
	"fmt"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/coordinator"
)

type Subscriber struct {
	coor  *coordinator.Coordinator
	dedup *Cache
	wg    sync.WaitGroup

	providers     []Provider
	pendingSwitch chan string
	errChan       chan error

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
		errChan:       make(chan error, 1),
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

func (s *Subscriber) reportFatal(err error) {
	select {
	case s.errChan <- err:
	default:
	}
}

func (s *Subscriber) Err() <-chan error {
	return s.errChan
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
