package blockstore

import (
	"math/big"
)

const (
	FeeBucketSize = 500_000_000 // 0.5 Gwei
	MaxWaitBlock  = 10
)

type BlockData struct {
	Number     uint64
	BaseFee    *big.Int
	GasLimit   uint64
	Txs        []TxInfo
	FeeBuckets []FeeBucketStat
}

type TxInfo struct {
	Hash      string
	Tip       uint64
	GasUsed   uint64
	GasWeight float64 // sqrt(gasUsed / gasLimit)
}

type FeeBucketStat struct {
	Bucket           uint32
	TxCount          uint32
	TotalWaitBlocks  uint64
	TotalWaitSeconds uint64
	WaitBlockCount   [16]uint32
}
