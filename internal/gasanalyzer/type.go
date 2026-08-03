package gasanalyzer

import (
	"time"
)

// 실제 측정값 정리
var GasPredictionTargets = []TargetPercentile{
	{
		Name:       "market",
		Percentile: 0.50,
		Index:      P50,
		GroupKey:   "p50",
	},
	{
		Name:       "fast",
		Percentile: 0.75,
		Index:      P75,
		GroupKey:   "p75",
	},
	{
		Name:       "urgent",
		Percentile: 0.90,
		Index:      P90,
		GroupKey:   "p90",
	},
}

var GasAnalysisTargets = []float64{
	0.20,
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
	P20 = iota
	P40
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

type TargetPercentile struct {
	Name       string
	Percentile float64
	Index      int
	GroupKey   string
}

type WeightPoint struct {
	Index  int
	Weight float64
}

var PredictionGroups = map[string][]WeightPoint{

	"p50": {
		{Index: P40, Weight: 1},
		{Index: P45, Weight: 1},
		{Index: P50, Weight: 4},
		{Index: P55, Weight: 1},
		{Index: P60, Weight: 1},
	},

	"p75": {
		{Index: P65, Weight: 1},
		{Index: P70, Weight: 4},
		{Index: P75, Weight: 10},
		{Index: P80, Weight: 4},
		{Index: P85, Weight: 1},
	},

	"p90": {
		//{Index: P80, Weight: 1},
		{Index: P85, Weight: 2},
		{Index: P90, Weight: 5},
		{Index: P95, Weight: 2},
	},
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
	PredictResult   map[string]GasLevel
	AnalyzerBlock   map[string]GasLevel
	AnalyzerPending map[string]GasLevel
	UpdatedAt       time.Time `json:"updatedAt"`
}

type BlockAnalysisData struct {
	BlockNumber uint64
	BaseFee     uint64
	NextBaseFee uint64
	GasUsed     uint64
	GasLimit    uint64
	Cutoff      uint64
	TipPool     []WeightedTip
	UpdatedAt   time.Time
}
