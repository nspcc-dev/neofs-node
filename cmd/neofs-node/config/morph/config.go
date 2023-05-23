package morphconfig

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection       = "morph"
	notarySubsection = "notary"

	// DialTimeoutDefault is a default dial timeout of morph chain client connection.
	DialTimeoutDefault = 5 * time.Second

	// CacheTTLDefault is a default value for cached values TTL.
	// It is 0, because actual default depends on block time.
	CacheTTLDefault = time.Duration(0)
)

// Endpoints returns list of the values of "endpoints" config parameter
// from "morph" section.
//
// Throws panic if list is empty.
func Endpoints(c *config.Config) []string {
	var endpointsDeprecated []string

	sub := c.Sub(subsection).Sub("rpc_endpoint")
	for i := 0; ; i++ {
		s := sub.Sub(strconv.FormatInt(int64(i), 10))
		addr := config.StringSafe(s, "address")
		if addr == "" {
			break
		}

		endpointsDeprecated = append(endpointsDeprecated, addr)
	}

	endpoints := config.StringSliceSafe(c.Sub(subsection), "endpoints")
	endpoints = append(endpoints, endpointsDeprecated...)

	if len(endpoints) == 0 {
		panic(fmt.Errorf("no morph chain RPC endpoints, see `morph.rpc_endpoint` section"))
	}
	return endpoints
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

// CacheTTL returns the value of "cache_ttl" config parameter
// from "morph" section.
//
// Returns CacheTTLDefault if value is zero or invalid. Supports negative durations.
func CacheTTL(c *config.Config) time.Duration {
	res := config.DurationSafe(c.Sub(subsection), "cache_ttl")
	if res != 0 {
		return res
	}

	return CacheTTLDefault
}
