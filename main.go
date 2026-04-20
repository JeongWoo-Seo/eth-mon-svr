package main

import (
	"context"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	url := "https://ethereum-sepolia-rpc.publicnode.com"
	client, err := ethclient.Dial(url)
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
