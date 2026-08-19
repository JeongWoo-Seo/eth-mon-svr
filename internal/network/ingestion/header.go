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
			slog.String("provider", provider.name),
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
			slog.String("provider", provider.name),
		)

		//block ws 오류 시 provider 변경
		nextProvider, ok := s.alternateProvider(provider.name)
		if ok {
			currentProvider = nextProvider.name

			// block 변경을 pending 관리에 알림
			s.notifyPendingSwitch(nextProvider.name)

			logger.Info(
				ctx,
				"change ethereum provider",
				slog.String("system", "ethereum"),
				slog.String("action", "change network"),
				slog.String("from", provider.name),
				slog.String("to", nextProvider.name),
			)
		}

		//reconnect delay
		timer := time.NewTimer(reconnectDelay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return

		case <-timer.C:
		}
	}
}

func (s *Subscriber) connectHeadAndStream(ctx context.Context, provider Provider) error {
	client, err := rpc.DialContext(ctx, provider.url)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", provider.name, err)
	}
	defer client.Close()

	ch := make(chan *types.Header, headBufferSize)

	sub, err := client.EthSubscribe(ctx, ch, "newHeads")
	if err != nil {
		return fmt.Errorf("failed to subscribe %s: %w", provider.name, err)
	}
	defer sub.Unsubscribe()

	lastHeaderAt := time.Now()

	//headerTimeout = 30 , 10 초단위로
	watchdog := time.NewTimer(watchdogInterval)
	defer watchdog.Stop()

	logger.Info(
		ctx,
		"ethereum header subscribed",
		slog.String("system", "ethereum"),
		slog.String("action", "subscribe"),
		slog.String("subscription", "Header"),
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

		case header := <-ch:
			if header == nil {
				continue
			}

			lastHeaderAt = time.Now()
			s.coor.PushHeader(header)

		case <-watchdog.C:
			if time.Since(lastHeaderAt) >= headerTimeout {
				return fmt.Errorf("%w: provider=%s last_header=%s", errHeaderTimeout, provider.name, lastHeaderAt.Format(time.RFC3339))
			}
		}
	}
}
