package mempool

import (
	"math/big"
	"time"
)

type PendingTxInfo struct {
	Hash         string
	GasFeeCap    *big.Int
	GasTipCap    *big.Int
	EffectiveTip uint64
	GasWeight    float64
	Timestamp    time.Time
}
