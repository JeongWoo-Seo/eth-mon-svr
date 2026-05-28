package gasanalyzer

import (
	"math/big"
	"time"
)

var gasPredictionTargets = []TargetPercentile{
	{"low", 0.30},
	{"market", 0.50},
	{"fast", 0.75},
	{"urgent", 0.90},
}

type TargetPercentile struct {
	Name  string
	Ratio float64
}

type WeightedTip struct {
	Tip    uint64
	Weight float64
}

type GasTip struct {
	Tip uint64
	Gas uint64
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
