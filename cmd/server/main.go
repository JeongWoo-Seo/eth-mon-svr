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
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/coordinator"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/grpcClient"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/ingestion"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
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
	rpcManager, err := rpcmanager.NewRpcManager(cfg.EthAlcRpcHttpUrl, cfg.EthChaRpcHttpUrl)
	if err != nil {
		logger.Error(ctx, "failed to connect ethereum rpc",
			err,
			"system", "ethereum",
			"action", "connect",
		)
		panic(err)
	}
	defer rpcManager.Close()

	logger.Info(ctx, "ethereum connected",
		slog.String("system", "ethereum"),
		slog.String("action", "connect"),
	)

	//////////////////////////
	// connect grpc
	//////////////////////////
	grpcClient, cleanup, err := grpcClient.NewGasPredictClient(ctx, cfg.GrpcServerAddr)
	if err != nil {
		logger.Error(ctx, "fail to connet gRPC ",
			err,
			slog.String("system", "grpc client"))
	}
	defer cleanup()

	//////////////////////////
	// set mempool pending and block
	//////////////////////////
	pendingPool, poolErr := mempool.NewPendingMemPool(cfg.EthSepoliaChainId, cfg.TxStoreBlockTTL)
	if poolErr != nil {
		logger.Error(ctx, "failed to initialize pending pool",
			err,
			slog.String("system", "mempool"))
		panic(err)
	}

	dedup, err := mempool.NewCache(10000, 4*time.Minute)
	if err != nil {
		logger.Error(ctx, "failed to initialize mempool cache",
			err,
			slog.String("system", "mempool"))
		panic(err)
	}

	blockPool := blockstore.NewBlockStore(cfg.MaxBlockCount)

	//////////////////////////
	// start processor
	//////////////////////////
	analyzer := gasanalyzer.NewAnalyzer(pendingPool, grpcClient)
	proc := processor.NewProcess(pendingPool, blockPool, rpcManager, analyzer, grpcClient)
	coor := coordinator.NewCoordinator(proc, cfg.MaxBlockCount)
	coor.Start(ctx)

	report.StartReporter(ctx)

	//////////////////////////
	// subscribe eth
	//////////////////////////
	sub := ingestion.NewSubscriber(cfg.EthAlcRpcWsUrl, cfg.EthChaRpcWsUrl, coor, dedup)
	sub.SubscriberStart(ctx)

	//////////////////////////
	// gas analysis
	//////////////////////////
	logger.Info(ctx, "Monitoring server started.")
	analyzer.Start(ctx)

	//////////////////////////
	// server shutdown
	//////////////////////////
	<-ctx.Done()
	logger.Info(context.Background(), "shutdown requested")

	sub.Wait()
	coor.Stop()

	logger.Info(context.Background(), "Shutting down server...")
}
