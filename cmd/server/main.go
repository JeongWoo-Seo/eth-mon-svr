package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/eth"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/ingestion"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/worker"
	"github.com/ethereum/go-ethereum/core/types"
)

func main() {
	//////////////////////////
	// load config
	//////////////////////////
	ctx := context.Background()
	cfg := config.LoadConfig()

	logger.New(logger.Config{
		Service: cfg.Service,
		Env:     cfg.Env,
		Level:   slog.LevelInfo,
	})

	//////////////////////////
	// connect eth
	//////////////////////////
	ethClient, err := eth.NewEthClient(cfg.EthRpcHttpUrl)
	if err != nil {
		logger.Error(ctx, "failed to connect ethereum rpc",
			err,
			"system", "ethereum",
			"action", "connect",
		)
		panic(err)
	}
	defer ethClient.Close()

	logger.Info(ctx, "ethereum connected",
		slog.String("system", "ethereum"),
		slog.String("action", "connect"),
	)

	//////////////////////////
	// set pool, processor, mempool
	//////////////////////////
	state := mempool.NewState()
	dedup, err := mempool.NewCache(10000, 2*time.Minute)
	if err != nil {
		logger.Error(ctx, "failed to initialize mempool cache",
			err,
			slog.String("system", "lru"))
		panic(err)
	}
	proc := processor.NewProcess(state, ethClient.EthClient)
	pool := worker.NewPool(8, proc)
	pool.Start(ctx)
	report.StartReporter(ctx)

	//////////////////////////
	// subscribe eth
	//////////////////////////
	headerChan := make(chan *types.Header)

	sub := ingestion.NewSubscriber(cfg.EthRpcWsUrl, headerChan, pool.Input(), dedup)
	sub.SubscriberStart(ctx)

	for {
		log.Println(<-headerChan)
	}

}
