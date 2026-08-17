package processor

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

func (p *Process) getLastestBlockHeader(ctx context.Context) (*types.Header, error) {
	if err := p.limiter.WaitN(ctx, getBlockReceiptCu); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRateLimiterWait, err)
	}

	var header *types.Header
	err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
		var err error
		header, err = client.HeaderByNumber(ctx, nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLatestBlockHeaderFetch, err)
	}

	if header == nil {
		return nil, ErrLatestBlockHeaderNil
	}

	if header.Number == nil {
		return nil, ErrLatestBlockNumberNil
	}

	return header, nil
}

func (p *Process) initialFromHeader(ctx context.Context, header *types.Header) error {
	receipt, err := p.fetchBlockReceipts(ctx, header.Hash().Hex())
	if err != nil {
		return fmt.Errorf("%w: block=%d hash=%s: %v", ErrBlockReceiptFetch, header.Number.Uint64(), header.Hash().Hex(), err)
	}

	if len(receipt) == 0 {
		return fmt.Errorf("%w: block=%d hash=%s", ErrBlockReceiptsEmpty, header.Number.Uint64(), header.Hash().Hex())
	}

	//block 데이터 생성
	blockData := p.CalculateBlockTxTip(header, receipt)

	// blockstore 저장
	p.blockstore.AddBlock(blockData)

	//분석을 위한 데이터 초기화
	p.UpdateBlockInfoForAnalysis(header)

	return nil
}
