package processor

import (
	"context"
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

//go:generate mockgen -destination=mocks/mock_rpc_manager.go -package=mocks . RPCManager
//go:generate mockgen -destination=mocks/mock_gas_analyzer.go -package=mocks . GasAnalyzer
//go:generate mockgen -destination=mocks/mock_pending_pool.go -package=mocks . PendingPool
//go:generate mockgen -destination=mocks/mock_gas_prediction_client.go -package=mocks . GasPredictionClient

// RPCManager is the subset of rpcmanager.RPCManager used by the processor.
type RPCManager interface {
	EthClientFunc(ctx context.Context, cost rpcmanager.RPCCost, fn func(client *ethclient.Client) error) error
	FetchBatch(ctx context.Context, cost rpcmanager.RPCCost, elems []rpc.BatchElem) error
}

// GasAnalyzer is the subset of gasanalyzer.Analyzer used by the processor.
type GasAnalyzer interface {
	EffectiveTipFromReceipt(effectiveGasPrice, baseFee *big.Int) (uint64, bool)
	CalculateWeightForGasUsed(gasUsed, gasLimit uint64) float64
	CalculateNextBaseFee(baseFee *big.Int, gasUsed, gasLimit uint64) *big.Int
	BlockPercentiles(poolData []gasanalyzer.WeightedTip) (map[string]uint64, uint64)
	GetCurrentBlockNumAndTime() (uint64, uint64)
	GetPrediction() gasanalyzer.GasPrediction
	Reset()
	SetReady()
	UpdateAnalBlockTxPredictionGasResult(result map[string]uint64)
	UpdateLatestBlockData(header *types.Header, nextBaseFee, cutoff uint64)
	DecayValues() []float64
}

// PendingPool is the subset of mempool.PendingMemPool used by the processor.
type PendingPool interface {
	Clear()
	PushBatch(txs []*types.Transaction, currentBlockNum, curBlockTime uint64)
	RemoveByReceipts(header *types.Header, receipts []*types.Receipt) ([]blockstore.FeeBucketStat, int)
	RemoveExpired(curBlock uint64) int
}

// GasPredictionClient is the subset of grpcClient.GasPredictionClient used by the processor.
type GasPredictionClient interface {
	FeeBucketSend(req *pb.FeeStatisticsRequest)
}
