package gasanalyzer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/ethclient"
)

func (a *Analyzer) CompareFeeHistory(client *ethclient.Client) {
	a.mu.Lock()
	preResult := a.latestResult
	a.mu.Unlock()

	ctx := context.Background()
	if preResult.NextBlockNumber == 0 || preResult.AnalyzerBlock == nil {
		logger.Info(ctx, "empty result data",
			"system", "analysis",
			"block_num", preResult.NextBlockNumber)
		return
	}

	//0.90 -> 90 으로 변환하기위한 과정
	per := make([]float64, 0, len(GasPredictionTargets))
	for _, t := range GasPredictionTargets {
		per = append(per, t.Percentile*100)
	}

	history, err := client.FeeHistory(ctx, 1, big.NewInt(int64(preResult.NextBlockNumber)), per)
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

		for i, t := range GasPredictionTargets {

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
