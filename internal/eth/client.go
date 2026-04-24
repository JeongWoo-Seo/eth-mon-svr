package eth

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Client struct {
	EthClient *ethclient.Client
}

func NewEthClient(url string) (*Client, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrEthDial, url)
	}

	return &Client{EthClient: client}, nil
}

func (c *Client) WatchHeaders(ctx context.Context, url string, ch chan<- *types.Header) {
	for {
		// 1. eth header 구독
		sub, err := c.EthClient.SubscribeNewHead(ctx, ch)
		if err != nil {
			logger.Error(ctx, ErrEthSubscribe.Error(),
				err,
				"event", "eth_newHeads",
				"action", "subscribe",
				"retry", true,
				"retry_after", 5*time.Second,
			)

			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case <-time.After(5 * time.Second):
				newClient, diarErr := ethclient.Dial(url)
				if diarErr == nil {
					c.EthClient.Close()
					c.EthClient = newClient
				}
			}
			continue
		}

		logger.Info(ctx, "ethereum subscription established",
			"event", "eth_newHeads",
			"status", "success",
		)

		// 2. 오류 발생시 기존 구독을 끊기
	waitLoop:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				logger.Warn(ctx, "eth subscription disconnected",
					err,
					"system", "ethereum",
					"event", "eth_newHeads",
					"action", "subscribe",
					"retry", true,
				)
				sub.Unsubscribe()
				break waitLoop
			}
		}
	}
}

func (c *Client) WatchPendingTransactions(ctx context.Context, url string, ch chan<- string) {
	for {
		sub, err := c.EthClient.Client().EthSubscribe(ctx, ch, "newPendingTransactions")
		if err != nil {
			logger.Error(ctx, ErrEthSubscribe.Error(),
				err,
				"event", "eth_newPendingTransactions",
				"action", "subscribe",
				"retry", true,
			)

			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case <-time.After(5 * time.Second):
				newClient, diarErr := ethclient.Dial(url)
				if diarErr == nil {
					c.EthClient.Close()
					c.EthClient = newClient
				}
			}
			continue
		}

		logger.Info(ctx, "newPendingTransactions subscription established",
			"event", "eth_newPendingTransactions",
			"status", "success",
		)

	waitLoop:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				logger.Warn(ctx, "eth subscription disconnected",
					err,
					"system", "ethereum",
					"event", "eth_newPendingTransactions",
					"action", "subscribe",
					"retry", true,
				)
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
		return nil, fmt.Errorf("%w: %v", ErrEthSuggestGasPrice, err)
	}

	return price, nil
}

func (c *Client) Close() {
	c.EthClient.Close()
}
