package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/eth"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/ingestion"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/worker"
)

func main() {
	//////////////////////////
	// load config
	//////////////////////////
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadConfig()

	logger.New(logger.Config{
		Service: cfg.Service,
		Env:     cfg.Env,
		Level:   slog.LevelInfo,
	})

	//////////////////////////
	// connect eth
	//////////////////////////
	alcEthClient, err := eth.NewEthClient(cfg.EthAlcRpcHttpUrl)
	if err != nil {
		logger.Error(ctx, "failed to connect ethereum rpc",
			err,
			"system", "ethereum",
			"action", "connect",
		)
		panic(err)
	}
	defer alcEthClient.Close()

	infEthClient, err := eth.NewEthClient(cfg.EthInfRpcHttpUrl)
	if err != nil {
		logger.Error(ctx, "failed to connect ethereum rpc",
			err,
			"system", "ethereum",
			"action", "connect",
		)
		panic(err)
	}
	defer infEthClient.Close()

	logger.Info(ctx, "ethereum connected",
		slog.String("system", "ethereum"),
		slog.String("action", "connect"),
	)

	//////////////////////////
	// set pool, processor, mempool
	//////////////////////////
	pendingPool := mempool.NewPendingMemPool(400, 20*time.Second)
	dedup, err := mempool.NewCache(10000, 2*time.Minute)
	if err != nil {
		logger.Error(ctx, "failed to initialize mempool cache",
			err,
			slog.String("system", "lru"))
		panic(err)
	}
	blockstore := blockstore.NewBlockStore(cfg.MaxBlockCount)

	gasOracle := gasanalyzer.NewGasOracle(pendingPool)
	analyzer := gasanalyzer.NewAnalyzer(cfg.Lamda, pendingPool, gasOracle)
	proc := processor.NewProcess(pendingPool, blockstore, alcEthClient.EthClient, infEthClient.EthClient, analyzer, gasOracle)
	pendingWorker := worker.NewPendingWorker(cfg.WorkerCount, proc)
	pendingWorker.Start(ctx)

	blockWorker := worker.NewBlockWorker(proc)
	blockWorker.Start(ctx)

	report.StartReporter(ctx)

	//////////////////////////
	// subscribe eth
	//////////////////////////
	sub := ingestion.NewSubscriber(cfg.EthAlcRpcWsUrl, cfg.EthInfRpcWsUrl, blockWorker.Input(), pendingWorker.Input(), dedup)
	sub.SubscriberStart(ctx)

	//////////////////////////
	// gas analysis
	//////////////////////////
	logger.Info(ctx, "Monitoring server started.")
	analyzer.Start(ctx)
	//gasOracle.Start(ctx)
	//////////////////////////
	// server shutdown
	//////////////////////////
	<-ctx.Done()

	logger.Info(context.Background(), "Shutting down server...")

	time.Sleep(2 * time.Second)
}
