package mempool

import "github.com/ethereum/go-ethereum/common"

const (
	MaxWaitBlock = 10
)

type PendingTx struct {
	Hash  common.Hash
	From  common.Address
	Nonce uint64

	TipCap   uint64
	FeeCap   uint64
	GasLimit uint64

	SeenBlock     uint64
	SeenBlockTime uint64
	ExpireBlock   uint64

	// ExpireIndex에서 O(1) 삭제를 위해 저장
	ExpireIndex int
	NonceGap    bool
}

type AccountPending struct {
	NonceMap map[uint64]*PendingTx
}
