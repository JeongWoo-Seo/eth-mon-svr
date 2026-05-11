package mempool

import (
	"math/big"
	"time"
)

type PendingTxInfo struct {
	Hash      string
	GasFeeCap *big.Int
	GasTipCap *big.Int
	Gas       uint64
	Timestamp time.Time
}
