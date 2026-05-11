package gasanalyzer

import (
	"math/big"
	"time"
)

type WeightedTip struct {
	Tip    uint64
	Weight float64
}

type GasLevel struct {
	PriorityFee *big.Int `json:"priorityFee"`
	MaxFee      *big.Int `json:"maxFee"`
}

type GasPrediction struct {
	BlockNumber uint64              `json:"blockNumber"`
	NextBaseFee *big.Int            `json:"nextBaseFee"`
	Levels      map[string]GasLevel `json:"levels"`
	UpdatedAt   time.Time           `json:"updatedAt"`
}
