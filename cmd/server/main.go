package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/coordinator"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/auth"
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
	rpcManager, err := rpcmanager.NewRpcManager(cfg.RPCs)
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
	authGrpcClient, err := auth.NewAuthGrpcClient(cfg.GrpcServerAddr)
	if err != nil {
		logger.Error(ctx, "fail to connect auth gRPC ",
			err,
			slog.String("system", "grpc client"))
		panic(err)
	}
	defer authGrpcClient.Close()

	tokenManager, err := auth.NewTokenManager(authGrpcClient, cfg.Service, cfg.AuthClientSecret)
	if err != nil {
		logger.Error(ctx, "fail to generate token manager",
			err,
			slog.String("system", "grpc client"))
		panic(err)
	}

	grpcClient, cleanup, err := grpcClient.NewGasPredictClient(ctx, cfg.GrpcServerAddr, tokenManager)
	if err != nil {
		logger.Error(ctx, "fail to connet gRPC ",
			err,
			slog.String("system", "grpc client"))
		panic(err)
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

	blockPool := blockstore.NewBlockStore(cfg.MaxBlockCount)

	//////////////////////////
	// start processor
	//////////////////////////
	analyzer := gasanalyzer.NewAnalyzer(pendingPool, grpcClient, cfg.MaxBlockCount)
	proc := processor.NewProcess(pendingPool, blockPool, rpcManager, analyzer, grpcClient)
	coor := coordinator.NewCoordinator(proc, cfg.MaxBlockCount)
	coor.Start(ctx)

	report.StartReporter(ctx)

	//////////////////////////
	// subscribe eth
	//////////////////////////
	sub, err := ingestion.NewSubscriber(cfg.WSs, coor)
	if err != nil {
		logger.Error(ctx, "failed to initialize pending tx cache",
			err,
			slog.String("system", "Subscribe"))
		panic(err)
	}
	sub.SubscriberStart(ctx)

	//////////////////////////
	// gas analysis
	//////////////////////////
	logger.Info(ctx, "Monitoring server started.")
	analyzer.Start(ctx)

	//////////////////////////
	// server shutdown
	//////////////////////////
	select {
	case err := <-sub.Err():
		logger.Error(ctx, "subscriber fatal error", err)
		stop()

	case <-ctx.Done():
	}

	logger.Info(context.Background(), "shutdown requested")

	sub.Wait()
	coor.Stop()

	logger.Info(context.Background(), "Shutting down server...")
}
