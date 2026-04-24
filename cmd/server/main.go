package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/eth"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/worker"
	"github.com/ethereum/go-ethereum/core/types"
)

func main() {
	ctx := context.Background()
	cfg := config.LoadConfig()

	logger.New(logger.Config{
		Service: cfg.Service,
		Env:     cfg.Env,
		Level:   slog.LevelInfo,
	})

	ethClient, err := eth.NewEthClient(cfg.EthRpcWsUrl)
	if err != nil {
		logger.Error(ctx, "failed to connect ethereum rpc",
			err,
			"system", "ethereum",
			"action", "connect",
		)
		os.Exit(1)
	}
	defer ethClient.Close()

	logger.Info(ctx, "ethereum connected",
		slog.String("system", "ethereum"),
		slog.String("action", "connect"),
	)

	state := mempool.NewState()
	proc := processor.NewProcess(state, ethClient.EthClient)
	pool := worker.NewPool(50, proc)
	pool.Start(ctx)

	headerChan := make(chan *types.Header)
	txHashChan := make(chan string, 1000)
	go ethClient.WatchHeaders(ctx, cfg.EthRpcWsUrl, headerChan)
	go ethClient.WatchPendingTransactions(ctx, cfg.EthRpcWsUrl, txHashChan)

	for {
		select {
		case header := <-headerChan:
			logger.Info(ctx, "new block received",
				slog.String("system", "ethereum"),
				slog.String("event", "block_watch"),
				slog.Uint64("block_number", header.Number.Uint64()),
			)

		case txHash := <-txHashChan:
			pool.Input() <- txHash
			logger.Info(ctx, "new pending transaction received",
				slog.String("system", "ethereum"),
				slog.String("event", "pending_transaction_watch"),
				slog.String("tx_hash", txHash),
			)
		}
	}
}
