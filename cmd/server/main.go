package main

import (
	"context"
	"log"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/eth"
	"github.com/ethereum/go-ethereum/core/types"
)

func main() {
	cfg := config.LoadConfig()

	ethClient, err := eth.NewEthClient(cfg.EthRpcWsUrl)
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	defer ethClient.Close()

	log.Printf("eth connect successfully")

	headerChan := make(chan *types.Header)
	go ethClient.WatchHeaders(context.Background(), cfg.EthRpcWsUrl, headerChan)

	for {
		select {
		case header := <-headerChan:
			log.Printf("📦 새 블록 수신: #%d", header.Number.Uint64())
		}
	}
}
