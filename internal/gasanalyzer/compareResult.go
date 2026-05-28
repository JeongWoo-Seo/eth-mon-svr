package gasanalyzer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/ethclient"
)

func (a *Analyzer) CompareFeeHistory(client *ethclient.Client) {
	a.mu.Lock()
	preResult := a.latestResult
	a.mu.Unlock()

	if preResult.NextBlockNumber == 0 || preResult.Levels == nil {
		return
	}

	ctx := context.Background()
	per := make([]float64, 0, len(gasPredictionTargets))

	for _, t := range gasPredictionTargets {
		per = append(per, t.Ratio)
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

		for i, t := range gasPredictionTargets {

			actualTip := reward[i].Uint64()

			if pred, ok := preResult.Levels[t.Name]; ok {

				diff := int64(pred.PriorityFee) -
					int64(actualTip)

				fmt.Printf(
					"%-10s | %-12d | %-12d | %-10d\n",
					t.Name,
					pred.PriorityFee,
					actualTip,
					diff,
				)

			} else {
				fmt.Printf(
					"%-10s | 데이터 없음\n", t.Name)
			}
		}

		fmt.Printf("BaseFee - 예측 : %d 실제 : %d \n", preResult.NextBaseFee, history.BaseFee[0].Uint64())
	}
}
