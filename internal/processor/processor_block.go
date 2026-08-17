package processor

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"slices"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	receiptChunkSize  = 30
	receiptCuPerTx    = 15
	getBlockReceiptCu = 250
)

func (p *Process) fetchReceiptsBatch(ctx context.Context, txs types.Transactions) ([]*types.Receipt, error) {
	if len(txs) == 0 {
		return nil, nil
	}

	receipts := make([]*types.Receipt, 0, txs.Len())

	for i := 0; i < txs.Len(); i += receiptChunkSize {
		end := i + receiptChunkSize
		if end > txs.Len() {
			end = txs.Len()
		}

		chunkTxs := txs[i:end]
		chunkSize := len(chunkTxs)
		chunkElems := make([]rpc.BatchElem, chunkSize)
		chunkReceipts := make([]*types.Receipt, chunkSize)

		for j, tx := range chunkTxs {
			chunkElems[j] = rpc.BatchElem{
				Method: "eth_getTransactionReceipt",
				Args:   []interface{}{tx.Hash()},
				Result: &chunkReceipts[j],
			}
		}

		totalCu := chunkSize * receiptCuPerTx
		if err := p.limiter.WaitN(ctx, totalCu); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRateLimiterWait, err)
		}

		err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
			return client.Client().BatchCallContext(ctx, chunkElems)
		})
		if err != nil {
			logger.Error(ctx, "Failed to fetch receipts batch chunk",
				err,
				slog.String("system", "ethereum"),
				slog.Int("chunk_start", i),
				slog.Int("chunk_size", chunkSize))
			return nil, err
		}

		for j := 0; j < chunkSize; j++ {
			if chunkElems[j].Error != nil {
				logger.Warn(ctx, "Failed to fetch individual tx receipt",
					slog.String("err", chunkElems[j].Error.Error()),
					slog.String("hash", chunkTxs[j].Hash().Hex()))
				continue
			}
			if chunkReceipts[j] != nil {
				receipts = append(receipts, chunkReceipts[j])
			}
		}
	}

	return receipts, nil
}

func (p *Process) fetchBlockReceipts(ctx context.Context, blockNumberHex string) ([]*types.Receipt, error) {
	var receipts []*types.Receipt

	if err := p.limiter.WaitN(ctx, getBlockReceiptCu); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRateLimiterWait, err)
	}

	err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
		return client.Client().CallContext(ctx, &receipts, "eth_getBlockReceipts", blockNumberHex)
	})
	if err != nil {
		logger.Error(ctx, "Failed to fetch block receipts",
			err,
			slog.String("block", blockNumberHex))
		return nil, err
	}

	return receipts, nil
}

func (p *Process) CalculateBlockTxTip(header *types.Header, receipts []*types.Receipt) blockstore.BlockData {
	blockData := blockstore.BlockData{
		Number:   header.Number.Uint64(),
		BaseFee:  header.BaseFee,
		GasLimit: header.GasLimit,
		Txs:      make([]blockstore.TxInfo, 0, len(receipts)),
	}

	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}

		tip, ok := p.gasanalyzer.EffectiveTipFromReceipt(receipt.EffectiveGasPrice, blockData.BaseFee)
		if !ok {
			continue
		}

		weight := p.gasanalyzer.CalculateWeightForGasUsed(receipt.GasUsed, blockData.GasLimit)
		blockData.Txs = append(blockData.Txs, blockstore.TxInfo{
			Hash:      receipt.TxHash.Hex(),
			Tip:       tip,
			GasUsed:   receipt.GasUsed,
			GasWeight: weight,
		})
	}

	return blockData
}

func (p *Process) CompareFeeHistory(ctx context.Context) {
	preResult := p.gasanalyzer.GetPrediction()

	if preResult.NextBlockNumber == 0 || preResult.AnalyzerBlock == nil {
		logger.Info(ctx, "empty result data",
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//0.90 -> 90 으로 변환하기위한 과정
	per := make([]float64, 0, len(gasanalyzer.GasPredictionTargets))
	for _, t := range gasanalyzer.GasPredictionTargets {
		per = append(per, t.Percentile*100)
	}

	if err := p.limiter.WaitN(ctx, feeHistoryCu); err != nil {
		logger.Error(ctx, "Rate limiter error in CompareFeeHistory",
			err,
			"system", "analysis",
			"requested_cu", feeHistoryCu)
		return
	}

	var history *ethereum.FeeHistory
	err := p.rpcManager.EthClientFunc(ctx, func(client *ethclient.Client) error {
		var err error
		history, err = client.FeeHistory(ctx, 1, big.NewInt(int64(preResult.NextBlockNumber)), per)
		return err
	})
	if err != nil {
		logger.Error(ctx, "failed to get block fee history",
			err,
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//결과 비교
	if len(history.Reward) > 0 && len(history.BaseFee) >= 2 {
		reward := history.Reward[0]

		for i, t := range gasanalyzer.GasPredictionTargets {

			actualTip := reward[i].Uint64()

			if _, ok := preResult.AnalyzerBlock[t.Name]; ok {
				anaBlock := int64(preResult.AnalyzerBlock[t.Name].PriorityFee)
				anaPending := int64(preResult.AnalyzerPending[t.Name].PriorityFee)

				blend := int64(preResult.PredictResult[t.Name].PriorityFee)
				diff := blend - int64(actualTip)

				sAnaBlock := humanize.Comma(anaBlock)
				sAnaPending := humanize.Comma(anaPending)
				sBlend := humanize.Comma(blend)
				sActual := humanize.Comma(int64(actualTip))
				sDiff := humanize.Comma(diff)
				if diff > 0 {
					sDiff = "+" + sDiff // 양수일 때 +
				}

				fmt.Printf(
					"%-10s | %-14s | %-14s | %-14s | %-14s | %-12s\n",
					t.Name,
					sAnaBlock,
					sAnaPending,
					sBlend,
					sActual,
					sDiff,
				)

			} else {
				fmt.Printf(
					"%-10s | 데이터 없음\n", t.Name)
			}
		}

		fmt.Printf("BaseFee - 예측 : %d 실제 : %d \n", preResult.NextBaseFee, history.BaseFee[0].Uint64())
	}
}

func (p *Process) ClearMempoolToReceipts(ctx context.Context, header *types.Header, receipts []*types.Receipt) []blockstore.FeeBucketStat {
	if len(receipts) == 0 {
		return []blockstore.FeeBucketStat{}
	}

	feeBucket, removedCnt := p.pendingPool.RemoveByReceipts(header, receipts)

	if removedCnt > 0 {
		logger.Info(ctx, "Transactions cleared from mempool",
			slog.Uint64("block_number", header.Number.Uint64()),
			slog.Int("removed_count", removedCnt),
		)
	}

	return feeBucket
}

func (p *Process) UpdateBlockInfoForAnalysis(header *types.Header) {
	if header.Number == nil {
		return
	}

	nextBaseFee := p.gasanalyzer.CalculateNextBaseFee(header.BaseFee, header.GasUsed, header.GasLimit)
	blockData := p.blockstore.GetBlockData()

	if len(blockData) == 0 {
		return
	}
	pool := make([]gasanalyzer.WeightedTip, 0, len(blockData)*300)

	for i, b := range blockData {
		if i >= len(p.gasanalyzer.DecayTable) {
			break
		}
		decay := p.gasanalyzer.DecayTable[i]

		for _, tx := range b.Txs {
			pool = append(pool, gasanalyzer.WeightedTip{
				Tip:    tx.Tip,
				Weight: tx.GasWeight * decay,
			})
		}
	}

	blockResult, cutoff := p.gasanalyzer.BlockPercentiles(pool)

	// 가스 분석을 위한 블록 정보 업데이트
	p.gasanalyzer.UpdateLatestBlockData(header, nextBaseFee.Uint64(), cutoff)

	//블록에 포함된 tx기반 가스 예측
	p.gasanalyzer.UpdateAnalBlockTxPredictionGasResult(blockResult)
}

func (p *Process) removeExpired(blockNum uint64) {
	removedCnt := p.pendingPool.RemoveExpired(blockNum)
	if removedCnt > 0 {
		logger.Info(context.Background(), "remove old txs from mempool",
			slog.Uint64("block_number", blockNum),
			slog.Int("removed_count", removedCnt),
		)
	}
}

func (p *Process) SendFeeBucketsToGrpc() {
	// data aggregate
	blockData := p.blockstore.GetBlockData()
	if len(blockData) == 0 {
		return
	}

	// fee bucket별 통계 집계
	stats := aggregateFeeBucket(blockData)
	if len(stats) == 0 {
		return
	}

	// FeeBucket -> protobuf 변환
	buckets := convertFeeBucketToProto(stats)
	if len(buckets) == 0 {
		return
	}

	// Bucket 번호 순으로 정렬
	slices.SortFunc(buckets, func(a, b *pb.FeeBucket) int {
		return cmp.Compare(a.Bucket, b.Bucket)
	})

	req := &pb.FeeStatisticsRequest{
		Buckets:   buckets,
		UpdatedAt: timestamppb.Now(),
	}

	// grpc send data(->ch)
	p.grpcClient.FeeBucketSend(req)
}

func aggregateFeeBucket(blockData []blockstore.BlockData) map[uint32]*blockstore.FeeBucketStat {
	stats := make(map[uint32]*blockstore.FeeBucketStat)

	for _, block := range blockData {
		if len(block.FeeBuckets) == 0 {
			continue
		}

		for _, bucket := range block.FeeBuckets {
			stat, ok := stats[bucket.Bucket]
			if !ok {
				stat = &blockstore.FeeBucketStat{
					Bucket: bucket.Bucket,
				}

				stats[bucket.Bucket] = stat
			}

			stat.TxCount += bucket.TxCount
			stat.TotalWaitBlocks += bucket.TotalWaitBlocks
			stat.TotalWaitSeconds += bucket.TotalWaitSeconds

			for i := 0; i < len(stat.WaitBlockCount); i++ {
				stat.WaitBlockCount[i] += bucket.WaitBlockCount[i]
			}
		}
	}

	return stats
}

func convertFeeBucketToProto(stats map[uint32]*blockstore.FeeBucketStat) []*pb.FeeBucket {
	buckets := make([]*pb.FeeBucket, 0, len(stats))

	for _, stat := range stats {
		if stat.TxCount == 0 {
			continue
		}

		minP, maxP := getFeeBucketRange(stat.Bucket)

		pbBucket := &pb.FeeBucket{
			Bucket:       stat.Bucket,
			MinPriority:  minP,
			MaxPriority:  maxP,
			TotalTxCount: stat.TxCount,

			AvrWaitBlocks:  float64(stat.TotalWaitBlocks) / float64(stat.TxCount),
			AvrWaitSeconds: float64(stat.TotalWaitSeconds) / float64(stat.TxCount),
		}

		//wait block count
		for waitBlock, txCount := range stat.WaitBlockCount {
			if txCount == 0 {
				continue
			}

			pbBucket.WaitBlockCount = append(pbBucket.WaitBlockCount,
				&pb.WaitBlockCount{
					WaitBlock: uint32(waitBlock),
					TxCount:   txCount,
				})
		}

		buckets = append(buckets, pbBucket)
	}

	return buckets
}

func getFeeBucketRange(bucket uint32) (float64, float64) {
	var weiPerGwei float64 = 1_000_000_000

	min := float64(blockstore.FeeBucketSize) / weiPerGwei * float64(bucket)
	max := min + float64(blockstore.FeeBucketSize)/weiPerGwei

	return min, max
}
