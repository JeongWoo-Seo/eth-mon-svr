package ingestion

import "errors"

var (
	errHeaderTimeout      = errors.New("ethereum header timeout")
	errSubscriptionClosed = errors.New("ethereum subscription closed")

	errPendingChannelClose        = errors.New("pending channel closed")
	errPendingSubscriptionTimeout = errors.New("pending Subscription timeout")
	errPendingSwitchChannelClose  = errors.New("pending channel switch closed")
)
