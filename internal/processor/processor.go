package processor

import (
	"context"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/report"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Process struct {
	state     *mempool.State
	ethClient *ethclient.Client
}

func NewProcess(state *mempool.State, client *ethclient.Client) *Process {
	return &Process{
		state:     state,
		ethClient: client,
	}
}

func (p *Process) GetTxInfo(hash common.Hash) {
	tx, _, err := p.ethClient.TransactionByHash(context.Background(), hash)
	if err != nil {
		return
	}

	report.IncTxFeched()
	p.state.Upset(tx)
}
