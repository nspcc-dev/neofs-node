package morphconfig

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection       = "morph"
	notarySubsection = "notary"

	// DialTimeoutDefault is a default dial timeout of morph chain client connection.
	DialTimeoutDefault = 5 * time.Second

	// NotaryDepositAmountDefault is a default deposit amount to notary contract.
	NotaryDepositAmountDefault = 5000_0000 // 0.5 Fixed8

	// NotaryDepositDurationDefault is a default deposit duration.
	NotaryDepositDurationDefault uint32 = 1000

	// MaxConnPerHostDefault is a default maximum of connections per host of the morph client.
	MaxConnPerHostDefault = 10
)

// RPCEndpoint returns list of the values of "rpc_endpoint" config parameter
// from "morph" section.
//
// Throws panic if list is empty.
func RPCEndpoint(c *config.Config) []string {
	v := config.StringSliceSafe(c.Sub(subsection), "rpc_endpoint")
	if len(v) == 0 {
		panic(fmt.Errorf("no morph chain RPC endpoints, see `morph.rpc_endpoint` section"))
	}

	return v
}

// DialTimeout returns the value of "dial_timeout" config parameter
// from "morph" section.
//
// Returns DialTimeoutDefault if the value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	if v > 0 {
		return v
	}

	return DialTimeoutDefault
}

// DisableCache returns the value of "disable_cache" config parameter
// from "morph" section.
func DisableCache(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "disable_cache")
}
