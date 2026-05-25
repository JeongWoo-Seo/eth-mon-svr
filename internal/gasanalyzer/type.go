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
	PriorityFee uint64 `json:"priorityFee"`
	MaxFee      uint64 `json:"maxFee"`
}

type GasPrediction struct {
	NextBlockNumber uint64              `json:"blockNumber"`
	NextBaseFee     uint64              `json:"nextBaseFee"`
	Levels          map[string]GasLevel `json:"levels"`
	UpdatedAt       time.Time           `json:"updatedAt"`
}

type BlockAnalysisData struct {
	BlockNumber uint64
	BaseFee     *big.Int
	NextBaseFee *big.Int
	GasUsed     uint64
	GasLimit    uint64
	TipPool     []WeightedTip
	UpdatedAt   time.Time
}
