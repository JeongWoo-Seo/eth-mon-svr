package rpcmanager

import (
	"time"

	"golang.org/x/time/rate"
)

const (
	ProviderAlchemy    string = "alchemy"
	ProviderChainstack string = "chainstack"

	RotateInterval = 30 * time.Second
)

type LimitType int

const (
	LimitCU LimitType = iota
	LimitRPS
)

type RPCPolicy struct {
	LimitType LimitType
	Limit     rate.Limit
	Burst     int
}

var rpcPolicies = map[string]RPCPolicy{
	ProviderAlchemy: {
		LimitType: LimitCU,
		Limit:     rate.Limit(450),
		Burst:     400,
	},

	ProviderChainstack: {
		LimitType: LimitRPS,
		Limit:     rate.Limit(22),
		Burst:     20,
	},
}

type RPCCost struct {
	CU  int
	RPC int
}
