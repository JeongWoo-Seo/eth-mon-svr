package report

import "sync/atomic"

type ErrorCount struct {
	TxRPCFailed           uint64
	TxIndividualRPCFailed uint64
	BlockReceiptRPCFailed uint64
	BlockProcessingFailed uint64
	BlockDropped          uint64
	FeeHistoryFailed      uint64
}

var E = &ErrorCount{}

func IncTxRPCFailed() {
	atomic.AddUint64(&E.TxRPCFailed, 1)
}

func IncTxIndividualRPCFailed() {
	atomic.AddUint64(&E.TxIndividualRPCFailed, 1)
}

func IncBlockReceiptRPCFailed() {
	atomic.AddUint64(&E.BlockReceiptRPCFailed, 1)
}

func IncBlockProcessingFailed() {
	atomic.AddUint64(&E.BlockProcessingFailed, 1)
}

func IncBlockDropped() {
	atomic.AddUint64(&E.BlockDropped, 1)
}

func IncFeeHistoryFailed() {
	atomic.AddUint64(&E.FeeHistoryFailed, 1)
}
