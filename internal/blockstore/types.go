package blockstore

import (
	"math/big"
)

type BlockData struct {
	Number   uint64
	BaseFee  *big.Int
	GasLimit uint64
	Txs      []TxInfo
}

type TxInfo struct {
	Hash      string
	Tip       uint64
	GasWeight float64 // sqrt(gasUsed / gasLimit)
}
