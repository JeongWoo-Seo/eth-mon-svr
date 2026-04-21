package eth

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Client struct {
	EthClient *ethclient.Client
}

func NewEthClient(url string) (*Client, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrConnectEthNode, err)
	}

	return &Client{EthClient: client}, nil
}

func (c *Client) WatchHeaders(ctx context.Context, url string, ch chan<- *types.Header) {
	for {
		sub, err := c.EthClient.SubscribeNewHead(ctx, ch)
		if err != nil {
			log.Printf("failed to subscribe and reconnect after 5s: %v")
			time.Sleep(5 * time.Second)
			newClient, diarErr := ethclient.Dial(url)
			if diarErr == nil {
				c.EthClient = newClient
			}
			continue
		}

		log.Println("subscribe eth")

	waitLoop:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				break
			case err := <-sub.Err():
				log.Printf("eth subscribe disconnect: %v", err)
				sub.Unsubscribe()
				break waitLoop
			}
		}
	}
}

// 권장 gas 가져오기
func (c *Client) GetSuggestGasPrice(ctx context.Context) (*big.Int, error) {
	price, err := c.EthClient.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("%v: %w", ErrGetSuggestGas, err)
	}

	return price, nil
}

func (c *Client) Close() {
	c.EthClient.Close()
}
