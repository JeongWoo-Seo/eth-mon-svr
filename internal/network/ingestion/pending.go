package ingestion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/rpc"
)

// pending tx

func (s *Subscriber) runPendingSub(ctx context.Context) {
	curProvider := ProviderChainstack

	rotation := time.NewTicker(pendingRotationInterval)
	defer rotation.Stop()

session:
	for {
		if ctx.Err() != nil {
			return
		}

		provider, ok := s.provider(curProvider)
		if !ok {
			nextProvider, exists := s.alternateProvider(curProvider)
			if !exists {
				logger.Error(ctx, "no ethereum pending provider available",
					errors.New("no provider"),
				)
				return
			}

			curProvider = nextProvider.name
			continue
		}

		logger.Info(ctx, "starting ethereum pending subscription",
			slog.String("system", "ethereum"),
			slog.String("action", "subscribe"),
			slog.String("subscription", "PendingTx"),
			slog.String("provider", provider.name),
		)

		streamCtx, cancel := context.WithCancel(ctx)
		errch := make(chan error, 1)
		go func() {
			errch <- s.connectPendingAndStream(streamCtx, provider)
		}()

		for {
			select {
			// 종료 신호
			case <-ctx.Done():
				cancel()
				select {
				case <-errch:
				case <-time.After(time.Second):
				}
				return

			// 주지적으로 provider 변경
			case <-rotation.C:
				nextProvider, ok := s.alternateProvider(provider.name)
				if !ok {
					continue
				}
				logger.Info(ctx, "rotating ethereum pending provider",
					slog.String("system", "ethereum"),
					slog.String("action", "rotate"),
					slog.String("from", provider.name),
					slog.String("to", nextProvider.name),
				)

				curProvider = nextProvider.name
				// 동작중인 ws 종료 요청
				cancel()

				// ws 종료 신호대기 최대 1초 대기
				select {
				case <-errch:
				case <-time.After(1 * time.Second):
				}
				continue session

			// block ws 장애로 인한 provier 변경
			case forced := <-s.pendingSwitch:
				//이미 변경할 provider인 경우
				if forced == provider.name {
					continue
				}

				nextProvider, ok := s.provider(forced)
				if !ok {
					continue
				}

				logger.Info(
					ctx,
					"forcing pending provider switch",
					slog.String("system", "ethereum"),
					slog.String("action", "failover"),
					slog.String("from", provider.name),
					slog.String("to", nextProvider.name),
				)

				curProvider = nextProvider.name

				//동작 중인 ws 종료
				cancel()

				select {
				case <-errch:
				case <-time.After(1 * time.Second):
				}

				continue session

			//ws 오류로 인해 종료된 경우
			case err := <-errch:
				if ctx.Err() != nil {
					cancel()
					return
				}

				logger.Error(ctx, "ethereum pending subscription failed",
					err,
					slog.String("system", "ethereum"),
					slog.String("action", "disconnect"),
					slog.String("subscription", "PendingTx"),
					slog.String("provider", provider.name),
				)

				nextProvider, ok := s.alternateProvider(provider.name)
				if ok {
					curProvider = nextProvider.name
				}

				cancel()

				timer := time.NewTimer(reconnectDelay)

				select {
				case <-ctx.Done():
					timer.Stop()
					return

				case <-timer.C:
				}
				continue session
			}
		}
	}
}

func (s *Subscriber) connectPendingAndStream(ctx context.Context, provider Provider) error {
	client, err := rpc.DialContext(ctx, provider.url)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", provider.name, err)
	}
	defer client.Close()

	ch := make(chan string, txBufferSize)

	sub, err := client.EthSubscribe(ctx, ch, "newPendingTransactions")
	if err != nil {
		return fmt.Errorf("failed to subscribe newPendingTransactions %s: %w",
			provider.name, err)
	}
	defer sub.Unsubscribe()

	logger.Info(ctx, "ethereum pending subscribed",
		slog.String("system", "ethereum"),
		slog.String("action", "subscribe"),
		slog.String("subscription", "PendingTx"),
		slog.String("provider", provider.name),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err, ok := <-sub.Err():
			if !ok || err == nil {
				return errSubscriptionClosed
			}
			return err

		case txHash := <-ch:
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
			default:
				logger.Warn(ctx, "pending tx channel full, dropping tx",
					slog.String("provider", string(provider.name)),
					slog.String("hash", txHash),
				)
			}
		}
	}
}
