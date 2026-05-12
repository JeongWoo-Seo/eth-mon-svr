package gasanalyzer

import (
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
	BlockNumber uint64              `json:"blockNumber"`
	NextBaseFee uint64              `json:"nextBaseFee"`
	Levels      map[string]GasLevel `json:"levels"`
	UpdatedAt   time.Time           `json:"updatedAt"`
}
