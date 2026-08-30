package ingestion

import "time"

const (
	connectRetryCount = 3

	pendingConnectRetryDelay = 3 * time.Second
	pendingRotationInterval  = 1 * time.Minute
	pendingReadyTimeout      = 5 * time.Second
	txDedupCasheSize         = 10000
	txBufferSize             = 50000

	headerTimeout           = 30 * time.Second
	watchdogInterval        = 10 * time.Second
	headBufferSize          = 100
	headerConnectRetryDelay = 1 * time.Second
)

const (
	ProviderAlchemy    string = "alchemy"
	ProviderChainstack string = "chainstack"
)

type Provider struct {
	Name string
	Url  string
}
