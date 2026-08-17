package processor

import "errors"

var (
	// RPC / Blockchain
	ErrLatestBlockHeaderNil   = errors.New("latest block header is nil")
	ErrLatestBlockNumberNil   = errors.New("latest block header number is nil")
	ErrLatestBlockHeaderFetch = errors.New("failed to fetch latest block header")

	// Block / Receipt
	ErrBlockReceiptsEmpty   = errors.New("block has no receipts")
	ErrBlockReceiptFetch    = errors.New("failed to fetch block receipts")
	ErrBlockDataCalculation = errors.New("failed to calculate block transaction tip")

	//limiter
	ErrRateLimiterWait = errors.New("rate limiter wait")
)
