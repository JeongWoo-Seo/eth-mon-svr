package gasanalyzer

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/ethereum/go-ethereum/ethclient"
)

var blendRatio = map[string]struct {
	History float64
	Pending float64
}{
	"low": {
		History: 0.80,
		Pending: 0.20,
	},
	"market": {
		History: 0.65,
		Pending: 0.35,
	},
	"fast": {
		History: 0.45,
		Pending: 0.55,
	},
	"urgent": {
		History: 0.30,
		Pending: 0.70,
	},
}

func (a *Analyzer) CompareFeeHistory(client *ethclient.Client) {
	a.mu.Lock()
	preResult := a.latestResult
	a.mu.Unlock()

	if preResult.NextBlockNumber == 0 || preResult.Levels1 == nil || preResult.Levels2 == nil {
		return
	}

	ctx := context.Background()
	per := make([]float64, 0, len(gasPredictionTargets))

	for _, t := range gasPredictionTargets {
		per = append(per, t.Ratio*100)
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

			if pred, ok := preResult.Levels1[t.Name]; ok {
				blockFee := int64(pred.PriorityFee)
				pendingFee := int64(preResult.Levels2[t.Name].PriorityFee)
				blend := calculateDynamicBlend(t.Name, blockFee, pendingFee)
				diff := blend - int64(actualTip)

				fmt.Printf(
					"%-10s | %-12d | %-12d| %-12d | %-12d | %-10d\n",
					t.Name,
					pred.PriorityFee,
					preResult.Levels2[t.Name].PriorityFee,
					blend,
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

func calculateDynamicBlend(levelName string, blockFee, pendingFee int64) int64 {
	// 두 값 중 하나가 0이거나 음수인 경우 예외 처리
	if blockFee <= 0 {
		return pendingFee
	}
	if pendingFee <= 0 {
		return blockFee
	}

	maxVal := math.Max(float64(blockFee), float64(pendingFee))
	minVal := math.Min(float64(blockFee), float64(pendingFee))

	// 1. 괴리율(Volatility Ratio) 계산
	// 두 지표가 얼마나 서로 다른 방향을 보고 있는지 측정 (0 = 일치, 1 = 극단적 괴리)
	volatility := (maxVal - minVal) / maxVal

	var w1 float64 // 블록 데이터 가중치

	// 2. 가스 레벨별 튜닝 파라미터 적용
	switch levelName {
	case "low":
		// Low 구간은 변동성이 크므로 두 값의 중간 스탠스를 유지하되 블록에 약간 더 무게를 둠
		w1 = 0.55 + (volatility * 0.20) // 0.55 ~ 0.75
	case "market":
		// Market은 안정성이 최우선. 블록 가중치를 높게 유지
		w1 = 0.60 + (volatility * 0.15) // 0.60 ~ 0.75
	case "fast":
		// Fast는 두 지표의 균형이 중요함
		w1 = 0.45 + (volatility * 0.20) // 0.45 ~ 0.65
	case "urgent":
		// Urgent 구간의 핵심: 첫 번째 샘플처럼 Pending이 비정상적으로 폭락할 때 방어해야 함
		if float64(blockFee)/float64(pendingFee) > 2.5 {
			// 블록이 Pending보다 2.5배 이상 크다면 Pending 큐의 왜곡(노이즈)으로 판단 -> 블록 데이터에 올인
			w1 = 0.85
		} else {
			// 평소에는 실시간 Pending 반영률을 높임
			w1 = 0.35 + (volatility * 0.25) // 0.35 ~ 0.60
		}
	default:
		w1 = 0.50
	}

	w2 := 1.0 - w1

	// 3. 1차 가중치 결합
	blend := (float64(blockFee) * w1) + (float64(pendingFee) * w2)

	// 4. 안전장치 (Clamping): Blend 결과가 양측 예측값의 [최저값 * 0.9]와 [최고값 * 1.1] 범위를 벗어나지 않도록 제한
	// 네 번째 샘플 market처럼 알고리즘이 튀어서 엉뚱한 값을 뱉는 현상을 완벽히 차단합니다.
	allowedMin := minVal * 0.9
	allowedMax := maxVal * 1.1

	if blend < allowedMin {
		blend = allowedMin
	} else if blend > allowedMax {
		blend = allowedMax
	}

	return int64(math.Round(blend))
}

func (o *GasOracle) CompareFeeHistory(client *ethclient.Client) {
	o.mu.Lock()
	preResult := o.latestResult
	o.mu.Unlock()

	if preResult.NextBlockNumber == 0 || preResult.Levels1 == nil || preResult.Levels2 == nil {
		return
	}

	ctx := context.Background()
	per := make([]float64, 0, len(gasPredictionTargets))

	for _, t := range gasPredictionTargets {
		per = append(per, t.Ratio*100)
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

			if pred, ok := preResult.Levels1[t.Name]; ok {
				blockFee := int64(pred.PriorityFee)
				pendingFee := int64(preResult.Levels2[t.Name].PriorityFee)
				blend := int64(float64(blockFee)*0.3 + float64(pendingFee)*0.7)
				diff := blend - int64(actualTip)

				fmt.Printf(
					"%-10s | %-12d | %-12d| %-12d | %-12d | %-10d\n",
					t.Name,
					pred.PriorityFee,
					preResult.Levels2[t.Name].PriorityFee,
					blend,
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
