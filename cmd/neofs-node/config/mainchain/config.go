package mainchainconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "mainchain"

	// DialTimeoutDefault is a default dial timeout of main chain client connection.
	DialTimeoutDefault = 5 * time.Second
)

// RPCEndpoint returns list of values of "rpc_endpoint" config parameter
// from "mainchain" section.
//
// Returns empty list if value is not a non-empty string array.
func RPCEndpoint(c *config.Config) []string {
	return config.StringSliceSafe(c.Sub(subsection), "rpc_endpoint")
}

// DialTimeout returns value of "dial_timeout" config parameter
// from "mainchain" section.
//
// Returns DialTimeoutDefault if value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	if v > 0 {
		return v
	}

	return DialTimeoutDefault
}
