package eth

import "errors"

var (
	ErrEthDial            = errors.New("ethereum dial failed")
	ErrEthSubscribe       = errors.New("ethereum subscription failed")
	ErrEthSuggestGasPrice = errors.New("failed to get suggest gas price")
)
