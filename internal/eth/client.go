package eth

import (
	"context"
	"fmt"
	"math/big"

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
