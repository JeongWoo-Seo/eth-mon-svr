package rpcmanager

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/time/rate"
)

type Client struct {
	EthClient *ethclient.Client
	url       string
	provider  string

	limitType LimitType
	limiter   *rate.Limiter
}

func NewEthClient(provider, url string, policy RPCPolicy) (*Client, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect eth: %s", provider)
	}

	return &Client{
		EthClient: client,
		url:       url,
		provider:  provider,
		limitType: policy.LimitType,
		limiter:   rate.NewLimiter(policy.Limit, policy.Burst),
	}, nil
}

func (c *Client) Close() {
	if c == nil || c.EthClient == nil {
		return
	}
	c.EthClient.Close()
}

func (c *Client) Wait(ctx context.Context, cost RPCCost) error {
	switch c.limitType {
	case LimitCU:
		return c.limiter.WaitN(ctx, cost.CU)
	case LimitRPS:
		return c.limiter.WaitN(ctx, cost.RPC)

	default:
		return fmt.Errorf("unsupported rpc limit type: %d", c.limitType)
	}
}
