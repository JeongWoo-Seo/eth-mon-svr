package gasanalyzer

import (
	"math/big"
	"time"
)

var GasPredictionTargets = []TargetPercentile{
	//{"market3", 0.50},
	{"fast", 0.75},
	{"urgent", 0.90},
}

var GasAnalysisTargets = []TargetPercentile{
	{"market1", 0.40},
	{"market2", 0.50},
	{"market3", 0.60},
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
	NextBlockNumber uint64 `json:"blockNumber"`
	NextBaseFee     uint64 `json:"nextBaseFee"`
	oracleBlock     map[string]GasLevel
	oraclePending   map[string]GasLevel
	analyzerBlock   map[string]GasLevel
	analyzerPending map[string]GasLevel
	UpdatedAt       time.Time `json:"updatedAt"`
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
