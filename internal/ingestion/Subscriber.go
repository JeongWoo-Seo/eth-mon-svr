package ingestion

import (
	"context"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	reconnectDelay = 5 * time.Second
	txBufferSize   = 50000
	headBufferSize = 100
)

type Subscriber struct {
	wsUrl      string
	headerChan chan<- *types.Header
	txHashChan chan<- string
	dedup      *mempool.Cache
}

func NewSubscriber(url string, headerChan chan<- *types.Header, txHashChan chan<- string, dedup *mempool.Cache) *Subscriber {
	return &Subscriber{
		wsUrl:      url,
		headerChan: headerChan,
		txHashChan: txHashChan,
		dedup:      dedup,
	}
}

func (s *Subscriber) SubscriberStart(ctx context.Context) {
	go subscription(ctx, s.wsUrl, "Header", "newHeads", headBufferSize, s.headerChan, nil)
	go subscription(ctx, s.wsUrl, "PendingTx", "newPendingTransactions", txBufferSize, s.txHashChan, s.dedup)
}

func subscription[T any](
	ctx context.Context,
	url string,
	label string,
	method string,
	buff int,
	outCh chan<- T,
	dedup *mempool.Cache,
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
	dedup *mempool.Cache,
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
				if txHash, ok := any(data).(string); ok {
					if dedup != nil && dedup.Seen(txHash) {
						logger.Info(ctx, "dedup seen")
						continue // 이미 본 트랜잭션은 채널에 넣지도 않고 무시
					}
				}
				report.IncPendginRecieved()
			}

			select {
			case outCh <- data:
			default:
				//pool이 꽉 찼을 때만 Drop
			}
		}
	}
}
