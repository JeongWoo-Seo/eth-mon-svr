package gasanalyzer

import (
	"math/big"
	"time"
)

var GasPredictionTargets = []TargetPercentile{
	{"market", 0.50},
	{"fast", 0.75},
	{"urgent", 0.90},
}

var GasAnalysisTargets = []float64{
	0.40,
	0.45,
	0.50,
	0.55,
	0.60,
	0.65,
	0.70,
	0.75,
	0.80,
	0.85,
	0.90,
	0.95,
}

const (
	P40 = iota
	P45
	P50
	P55
	P60
	P65
	P70
	P75
	P80
	P85
	P90
	P95
)

type WeightPoint struct {
	Index  int
	Weight float64
}

var PredictionGroups = map[string][]WeightPoint{
	"p50": {
		{P45, 1},
		{P50, 2},
		{P55, 1},
	},
	"p75": {
		{P70, 1},
		{P75, 8},
		{P80, 2},
	},
	"p90": {
		{P85, 1},
		{P90, 3},
		{P95, 1},
	},
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
