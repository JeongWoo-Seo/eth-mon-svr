package eth

import "errors"

var (
	ErrConnectEthNode       = errors.New("failed to connect eth node")
	ErrEthNewHeaderSubscibe = errors.New("failed to subscribe")
	ErrGetSuggestGas        = errors.New("failed to get suggest gas price")
)
