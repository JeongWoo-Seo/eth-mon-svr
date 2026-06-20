package gasanalyzer

import (
	"context"
	"log/slog"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
)

type GasOracle struct {
	BlockHist       *Histogram
	PendingHist     *Histogram
	mu              sync.RWMutex
	latestResult    GasPrediction
	latestBlockData BlockAnalysisData

	pendingPool *mempool.PendingMemPool
}

func NewGasOracle(pendingPool *mempool.PendingMemPool) *GasOracle {
	return &GasOracle{
		BlockHist:   NewHistogram(),
		PendingHist: NewHistogram(),
		pendingPool: pendingPool,
		latestBlockData: BlockAnalysisData{
			NextBaseFee: new(big.Int),
			BaseFee:     new(big.Int),
		},
	}
}

func (o *GasOracle) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	logger.Info(ctx, "Gas analyzer started")
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "Gas analyzer stopped")
			return
		case <-ticker.C:
			o.GasPrediction()
		}
	}
}

func (o *GasOracle) UpdateLatestBlockData(
	blockNumber uint64,
	baseFee *big.Int,
	gasUsed, gasLimit uint64,
	nextBaseFee *big.Int,
) {
	o.mu.Lock()
	defer o.mu.Unlock()

	//deap copy
	tmBaseFee := new(big.Int).Set(baseFee)
	tmNextBaseFee := new(big.Int).Set(nextBaseFee)

	o.latestBlockData = BlockAnalysisData{
		BlockNumber: blockNumber,
		BaseFee:     tmBaseFee,
		NextBaseFee: tmNextBaseFee,
		GasUsed:     gasUsed,
		GasLimit:    gasLimit,
		UpdatedAt:   time.Now(),
	}
}

func (o *GasOracle) GasPrediction() {
	o.mu.RLock()
	nextBlockNum := o.latestBlockData.BlockNumber + 1
	nextBaseFee := new(big.Int).Set(o.latestBlockData.NextBaseFee)
	o.mu.RUnlock()

	// Pending Histogram 갱신
	pendingTips := o.collectPendingTx(o.latestBlockData.NextBaseFee)

	o.PendingHist.Reset()
	o.PendingHist.Add(pendingTips)

	// Percentile 계산
	blockPrice, blockErr := o.BlockHist.PercentileGas(GasPredictionTargets)
	pendingPrice, pendingErr := o.PendingHist.PercentileGas(GasPredictionTargets)

	// 둘 다 실패한 경우만 에러
	if blockErr != nil && pendingErr != nil {
		logger.Error(context.Background(), "failed gas prediction", blockErr)
	}

	// nil map 방지
	if blockPrice == nil {
		blockPrice = make(map[string]uint64)
	}

	if pendingPrice == nil {
		pendingPrice = make(map[string]uint64)
	}

	o.UpdateResult(
		nextBlockNum,
		nextBaseFee,
		blockPrice,
		pendingPrice,
	)

	logger.Info(
		context.Background(),
		"Gas analysis complete",
		slog.String("system", "analysis"),
		slog.Uint64("next_block_number", nextBlockNum),
	)
}

func (o *GasOracle) collectPendingTx(nextBaseFee *big.Int) []GasTip {
	pendingData := o.pendingPool.Snapshot()

	pool := make([]GasTip, 0, len(pendingData))

	for _, tx := range pendingData {

		// 이미 포함 불가능한 tx
		if tx.GasFeeCap.Cmp(nextBaseFee) <= 0 {
			continue
		}

		feeMinusBase := new(big.Int).Sub(tx.GasFeeCap, nextBaseFee)

		tip := tx.GasTipCap
		if feeMinusBase.Cmp(tip) < 0 {
			tip = feeMinusBase
		}

		if !tip.IsUint64() {
			continue
		}
		adjustedGas := uint64(math.Round(math.Sqrt(float64(tx.Gas))))
		pool = append(pool, GasTip{
			Tip: tip.Uint64(),
			Gas: adjustedGas,
		})
	}

	return pool
}

func (o *GasOracle) UpdateResult(nextBlockNum uint64, nextBaseFee *big.Int, result1 map[string]uint64, result2 map[string]uint64) {
	u64NextBaseFee := nextBaseFee.Uint64()
	levels1 := make(map[string]GasLevel)
	for p, r := range result1 {
		levels1[p] = GasLevel{
			PriorityFee: r,
			MaxFee:      u64NextBaseFee + r,
		}
	}

	levels2 := make(map[string]GasLevel)
	for p, r := range result2 {
		levels2[p] = GasLevel{
			PriorityFee: r,
			MaxFee:      u64NextBaseFee + r,
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.latestResult = GasPrediction{
		NextBlockNumber: nextBlockNum,
		NextBaseFee:     u64NextBaseFee,

		UpdatedAt: time.Now(),
	}
}

func CalculateWeightForGasUsed(gasUsed uint64) float64 {
	return math.Sqrt(float64(gasUsed))
}
