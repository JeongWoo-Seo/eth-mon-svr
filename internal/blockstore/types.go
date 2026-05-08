package blockstore

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type BlockData struct {
	Number   uint64
	BaseFee  *big.Int
	GasLimit uint64
	Txs      []TxInfo
}

type TxInfo struct {
	Hash      common.Hash
	Tip       uint64
	GasUsed   uint64
	GasWeight float64 // sqrt(gasUsed / gasLimit)
}
