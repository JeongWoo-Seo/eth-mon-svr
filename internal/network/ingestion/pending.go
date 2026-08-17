package ingestion

import (
	"context"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/rpc"
)

// pending tx
type pendingSession struct {
	provider Provider
	cancel   context.CancelFunc
	ready    chan struct{} //세션 연결 완료 채널
	done     chan error    //세션 연결 종료 채널
}

func (s *Subscriber) runPendingSub(ctx context.Context) {
	// 첫번째 provider 적용
	curProvider := s.providers()[0]

	session, err := s.startPendingSession(ctx, curProvider)
	if err != nil {
		logger.Error(ctx, "initial pending session failed",
			err,
			slog.String("system", "ethereum"),
			slog.String("action", "subscribe"),
			slog.String("Provider", curProvider.name),
		)
		return
	}

	rotation := time.NewTicker(pendingRotationInterval)
	defer rotation.Stop()

	for {
		select {
		// 종료 신호
		case <-ctx.Done():
			session.cancel()
			return

		// 주지적으로 provider 변경
		case <-rotation.C:
			nextProvider, ok := s.alternateProvider(session.provider.name)
			if !ok {
				continue
			}

			nextSession, ok := s.handoverPending(ctx, session, nextProvider)
			if ok {
				logger.Info(ctx, "rotation pending provider switch",
					slog.String("system", "ethereum"),
					slog.String("action", "handover"),
					slog.String("from", session.provider.name),
					slog.String("to", nextSession.provider.name),
				)
				session = nextSession
			}

		// block ws 장애로 인한 provier 변경 요청
		case forced, ok := <-s.pendingSwitch:
			if !ok { //채널이 닫힌 경우
				logger.Error(ctx, "pending switch channel closed",
					errPendingSwitchChannelClose,
					slog.String("system", "ethereum"),
				)
				session.cancel()
				return
			}

			//이미 변경할 provider로 동작 중인 경우
			if forced == session.provider.name {
				continue
			}

			//연결할 다른 provider가 없는 경우
			nextProvider, ok := s.provider(forced)
			if !ok {
				continue
			}

			nextSession, ok := s.handoverPending(ctx, session, nextProvider)
			if ok {
				logger.Info(
					ctx,
					"forcing pending provider switch",
					slog.String("system", "ethereum"),
					slog.String("action", "handover"),
					slog.String("from", session.provider.name),
					slog.String("to", nextSession.provider.name),
				)
				session = nextSession
			}

		//ws 오류로 인해 종료된 경우
		case err, ok := <-session.done:
			if !ok {
				continue
			}

			logger.Error(ctx, "ethereum pending subscription disconnect",
				err,
				slog.String("system", "ethereum"),
				slog.String("action", "unsubscribe"),
				slog.String("subscription", "PendingTx"),
				slog.String("provider", session.provider.name),
			)

			for {
				nextProvider, ok := s.alternateProvider(session.provider.name)
				if !ok {
					return
				}

				nextSession, ok := s.handoverPending(ctx, session, nextProvider)
				if ok {
					session = nextSession
					logger.Info(ctx, "ethereum pending subscription reconnect",
						slog.String("system", "ethereum"),
						slog.String("action", "subscribe"),
						slog.String("provider", session.provider.name),
					)
					break
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
			}
		}
	}
}

func (s *Subscriber) handoverPending(ctx context.Context, old *pendingSession, firstProvider Provider) (*pendingSession, bool) {
	provider := firstProvider

	for i := 0; i < len(s.providers()); i++ {
		if ctx.Err() != nil {
			return old, false
		}

		nextSession, err := s.startPendingSession(ctx, provider)
		if err == nil {
			old.cancel()

			select {
			case <-old.done:
			case <-time.After(1 * time.Second):
			}
			return nextSession, true
		}

		if ctx.Err() != nil {
			return old, false
		}

		logger.Warn(ctx, "failed to subscribe pending",
			slog.String("system", "ethereum"),
			slog.String("action", "subscribe"),
			slog.String("provider", provider.name),
			slog.Any("error", err),
		)

		nextProvider, ok := s.alternateProvider(provider.name)
		if !ok {
			break
		}
		provider = nextProvider
	}

	return old, false
}

func (s *Subscriber) startPendingSession(ctx context.Context, provider Provider) (*pendingSession, error) {
	streamCtx, cancel := context.WithCancel(ctx)

	session := &pendingSession{
		provider: provider,
		cancel:   cancel,
		ready:    make(chan struct{}),
		done:     make(chan error, 1),
	}

	go func() {
		session.done <- s.connectPendingAndStream(streamCtx, session)
	}()

	select {
	case <-session.ready:
		return session, nil

	case err := <-session.done:
		cancel()
		return nil, err

	case <-time.After(pendingReadyTimeout):
		cancel()
		return nil, errPendingSubscriptionTimeout

	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}

func (s *Subscriber) connectPendingAndStream(ctx context.Context, session *pendingSession) error {
	client, err := rpc.DialContext(ctx, session.provider.url)
	if err != nil {
		return err
	}
	defer client.Close()

	ch := make(chan string, txBufferSize)

	sub, err := client.EthSubscribe(ctx, ch, "newPendingTransactions")
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	logger.Info(ctx, "ethereum pending subscribed",
		slog.String("system", "ethereum"),
		slog.String("action", "subscribe"),
		slog.String("subscription", "PendingTx"),
		slog.String("provider", session.provider.name),
	)

	//연결 완료 ch
	close(session.ready)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err, ok := <-sub.Err():
			if !ok || err == nil {
				return errSubscriptionClosed
			}
			return err

		case txHash, ok := <-ch:
			if !ok {
				return errPendingChannelClose
			}

			if txHash == "" {
				continue
			}

			report.IncPendginRecieved()

			if s.dedup != nil {
				if s.dedup.Seen(txHash) {
					continue
				}
			}

			select {
			case s.txHashChan <- txHash:
			case <-ctx.Done():
				return ctx.Err()
			default:
				logger.Warn(ctx, "pending tx channel full, dropping tx",
					slog.String("system", "ethereum"),
					slog.String("action", "dropp"),
					slog.String("tx hash", txHash),
				)
			}
		}
	}
}
