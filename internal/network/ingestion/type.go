package ingestion

import "time"

const (
	pendingRotationInterval = 30 * time.Second
	pendingReadyTimeout     = 5 * time.Second
	txBufferSize            = 50000

	headerTimeout    = 30 * time.Second
	watchdogInterval = 10 * time.Second
	headBufferSize   = 100

	reconnectDelay = 1 * time.Second
)

const (
	ProviderAlchemy    string = "alchemy"
	ProviderChainstack string = "chainstack"
)
