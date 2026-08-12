package rpcmanager

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
)

type Client struct {
	EthClient *ethclient.Client
	url       string
	provider  string
}

func NewEthClient(provider, url string) (*Client, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect eth: %s", provider)
	}

	return &Client{
		EthClient: client,
		url:       url,
		provider:  provider,
	}, nil
}

func (c *Client) Close() {
	if c == nil || c.EthClient == nil {
		return
	}
	c.EthClient.Close()
}
