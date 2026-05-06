package processor

import (
	"context"
	"log/slog"
	"sync"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Process struct {
	state     *mempool.State
	ethClient *ethclient.Client
}

var batchElemPool = sync.Pool{
	New: func() interface{} {
		e := make([]rpc.BatchElem, 0, 100)
		return &e
	},
}

var txResultPool = sync.Pool{
	New: func() interface{} {
		r := make([]*types.Transaction, 0, 100)
		return &r
	},
}

// 이더리움 메인넷 평균 트랜잭션은 150~300개 사이입니다.
var txHashPool = sync.Pool{
	New: func() interface{} {

		b := make([]string, 0, 300)
		return &b
	},
}

func NewProcess(state *mempool.State, client *ethclient.Client) *Process {
	return &Process{
		state:     state,
		ethClient: client,
	}
}

func (p *Process) GetTxInfo(hashes []common.Hash) {
	if len(hashes) == 0 {
		return
	}

	ePtr := batchElemPool.Get().(*[]rpc.BatchElem)
	rPtr := txResultPool.Get().(*[]*types.Transaction)
	elems := (*ePtr)[:0]
	results := (*rPtr)[:0]

	if cap(elems) < len(hashes) {
		elems = make([]rpc.BatchElem, len(hashes))
		results = make([]*types.Transaction, len(hashes))
	} else {
		elems = elems[:len(hashes)]
		results = results[:len(hashes)]
	}

	for i, hash := range hashes {
		results[i] = nil // 이전 결과 초기화
		elems[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{hash},
			Result: &results[i],
		}
	}

	ctx := context.Background()
	err := p.ethClient.Client().BatchCallContext(ctx, elems)
	if err != nil {
		logger.Error(ctx, "Batch RPC call failed",
			err,
			slog.String("system", "ethereum"),
			slog.String("action", "batch_get_tx"),
			slog.Int("batch_size", len(hashes)),
		)
		return
	}

	p.state.UpsetBulk(results)

	for i := range results {
		results[i] = nil
	}

	*ePtr = elems
	*rPtr = results
	batchElemPool.Put(ePtr)
	txResultPool.Put(rPtr)
}

func (p *Process) GetBlockByHash(header *types.Header) {
	ctx := context.Background()

	block, err := p.ethClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		logger.Error(ctx, "Failed to fetch block by hash",
			err,
			slog.String("system", "ethereum"),
			slog.String("block_hash", header.Hash().Hex()))
		return
	}

	txs := block.Transactions()
	if len(txs) == 0 {
		return
	}

	pSlicePtr := txHashPool.Get().(*[]string) //메모리 가져오기
	txHashes := *pSlicePtr
	txHashes = txHashes[:0]

	for _, tx := range txs {
		txHashes = append(txHashes, tx.Hash().Hex())
	}
	removedCnt := p.state.DeleteBulk(txHashes)

	*pSlicePtr = txHashes     //혹시라도 트랜잭션이 너무 많아 슬라이스의 Capacity가 늘어났다면, 슬라이스 헤더 정보(포인터, 길이, 용량)가 변경
	txHashPool.Put(pSlicePtr) // 반납하기

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", block.NumberU64()),
			slog.Int("removed_count", removedCnt),
		)
	}
}
