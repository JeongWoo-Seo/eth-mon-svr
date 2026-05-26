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
	per := []float64{30, 50, 75, 90}
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

		com := []struct {
			name  string
			index int
		}{
			{"low", 0},
			{"market", 1},
			{"fast", 2},
			{"urgent", 3},
		}

		for _, c := range com {
			actualTip := reward[c.index].Uint64()

			if pred, ok := preResult.Levels[c.name]; ok {
				diff := int64(pred.PriorityFee) - int64(actualTip)

				fmt.Printf("%-10s | %-12d | %-12d | %-10d\n",
					c.name, pred.PriorityFee, actualTip, diff)
			} else {
				// 키가 매칭되지 않을 경우를 대비한 로그
				fmt.Printf("%-10s | 데이터 없음\n", c.name)
			}
		}

		fmt.Printf("BaseFee - 예측 : %d 실제 : %d \n", preResult.NextBaseFee, history.BaseFee[0].Uint64())
	}
}
