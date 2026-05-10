package mempool

import (
	"math/big"
	"time"
)

type PendingTxInfo struct {
	Hash      string
	GasFeeCap *big.Int
	GasTipCap *big.Int
	Timestamp time.Time
}
