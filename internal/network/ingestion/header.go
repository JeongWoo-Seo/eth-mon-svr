package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

//header

func (s *Subscriber) runHeaderSub(ctx context.Context) {
	currentProvider := ProviderChainstack
	for {
		if ctx.Err() != nil {
			return
		}

		provider, ok := s.provider(currentProvider)
		if !ok {
			currentProvider = ProviderAlchemy
			continue
		}

		logger.Info(ctx, "starting ethereum header subscription",
			slog.String("system", "ethereum"),
			slog.String("action", "subscribe"),
			slog.String("subscription", "Header"),
			slog.String("provider", provider.Name),
		)

		err := s.connectHeadAndStream(ctx, provider)
		if ctx.Err() != nil {
			return
		}

		logger.Error(ctx, "ethereum header subscription failed",
			err,
			slog.String("system", "ethereum"),
			slog.String("action", "disconnect"),
			slog.String("subscription", "Header"),
			slog.String("provider", provider.Name),
		)

		//block ws 오류 시 provider 변경
		nextProvider, ok := s.alternateProvider(provider.Name)
		if ok {
			currentProvider = nextProvider.Name

			// block 변경을 pending 관리에 알림
			s.notifyPendingSwitch(nextProvider.Name)

			logger.Info(
				ctx,
				"change ethereum provider",
				slog.String("system", "ethereum"),
				slog.String("action", "change network"),
				slog.String("from", provider.Name),
				slog.String("to", nextProvider.Name),
			)
		}

		//reconnect delay
		timer := time.NewTimer(headerConnectRetryDelay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return

		case <-timer.C:
		}
	}
}

func (s *Subscriber) connectInitialHeader(ctx context.Context) (Provider, error) {
	var lastErr error

	for _, provider := range s.providers {
		for i := 1; i <= connectRetryCount; i++ {
			if ctx.Err() != nil {
				return Provider{}, ctx.Err()
			}

			logger.Info(ctx, "starting ethereum header subscription",
				slog.String("system", "ethereum"),
				slog.String("action", "subscribe"),
				slog.String("subscription", "Header"),
				slog.String("provider", provider.Name),
				slog.Int("attempt", i),
			)

			err := s.connectHeadAndStream(ctx, provider)
			if err == nil {
				return provider, nil
			}

			lastErr = err

			if i < connectRetryCount {
				select {
				case <-ctx.Done():
					return Provider{}, ctx.Err()

				case <-time.After(headerConnectRetryDelay):
				}
			}
		}

		logger.Error(ctx, "block provider exhausted retries",
			lastErr,
			slog.String("system", "ethereum"),
			slog.String("action", "subscribe"),
			slog.String("provider", provider.Name),
		)
	}

	return Provider{}, lastErr
}

func (s *Subscriber) connectHeadAndStream(ctx context.Context, provider Provider) error {
	client, err := rpc.DialContext(ctx, provider.Url)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", provider.Name, err)
	}
	defer client.Close()

	ch := make(chan *types.Header, headBufferSize)

	sub, err := client.EthSubscribe(ctx, ch, "newHeads")
	if err != nil {
		return fmt.Errorf("failed to subscribe %s: %w", provider.Name, err)
	}
	defer sub.Unsubscribe()

	logger.Info(
		ctx,
		"ethereum header subscribed",
		slog.String("system", "ethereum"),
		slog.String("action", "subscribe"),
		slog.String("subscription", "Header"),
		slog.String("provider", provider.Name),
	)

	return consumeHeaderStream(ctx, provider, ch, sub.Err(), watchdogInterval, headerTimeout, s.coor.PushHeader)
}

func consumeHeaderStream(
	ctx context.Context,
	provider Provider,
	headers <-chan *types.Header,
	errs <-chan error,
	watchdogInterval time.Duration,
	headerTimeout time.Duration,
	push func(*types.Header),
) error {
	lastHeaderAt := time.Now()

	//headerTimeout = 30 , 10 초단위로 확인
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err, ok := <-errs:
			if !ok || err == nil {
				return errSubscriptionClosed
			}
			return err

		case header := <-headers:
			if header == nil {
				continue
			}

			lastHeaderAt = time.Now()
			push(header)

		case <-ticker.C:
			if time.Since(lastHeaderAt) >= headerTimeout {
				return fmt.Errorf("%w: provider=%s last_header=%s", errHeaderTimeout, provider.Name, lastHeaderAt)
			}
		}
	}
}
