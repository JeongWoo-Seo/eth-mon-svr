package main

import (
	"context"
	"log"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/config"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	cfg := config.LoadConfig()
	client, err := ethclient.Dial(cfg.EthRpcUrl)
	if err != nil {
		log.Fatalf("can not connect eth: %v", err)
	}

	log.Println("success connet eth")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	price, err := client.SuggestGasPrice(ctx)
	if err != nil {
		log.Fatalf("can't get gas price: %v", err)
	}

	log.Printf("gas price: %s wei", price.String())
}
