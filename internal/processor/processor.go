package processor

import (
	"context"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Process struct {
	state     *mempool.State
	ethClient *ethclient.Client
}

func NewProcess(client *ethclient.Client) *Process {
	return &Process{
		ethClient: client,
	}
}

func (p *Process) GetTxInfo(hash common.Hash) {
	tx, _, err := p.ethClient.TransactionByHash(context.Background(), hash)
	if err != nil {
		return
	}

	p.state.Upset(tx)
}
