package morphconfig

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const (
	subsection       = "morph"
	notarySubsection = "notary"

	// DialTimeoutDefault is a default dial timeout of morph chain client connection.
	DialTimeoutDefault = 5 * time.Second

	// PriorityDefault is a default endpoint priority for the morph client.
	PriorityDefault = 1

	// CacheTTLDefault is a default value for cached values TTL.
	// It is 0, because actual default depends on block time.
	CacheTTLDefault = time.Duration(0)

	// SwitchIntervalDefault is a default Neo RPCs switch interval.
	SwitchIntervalDefault = 2 * time.Minute
)

// RPCEndpoint returns list of the values of "rpc_endpoint" config parameter
// from "morph" section.
//
// Throws panic if list is empty.
func RPCEndpoint(c *config.Config) []client.Endpoint {
	var es []client.Endpoint

	sub := c.Sub(subsection).Sub("rpc_endpoint")
	for i := 0; ; i++ {
		s := sub.Sub(strconv.FormatInt(int64(i), 10))
		addr := config.StringSafe(s, "address")
		if addr == "" {
			break
		}

		priority := int(config.IntSafe(s, "priority"))
		if priority <= 0 {
			priority = PriorityDefault
		}

		es = append(es, client.Endpoint{
			Address:  addr,
			Priority: priority,
		})
	}

	if len(es) == 0 {
		panic(fmt.Errorf("no morph chain RPC endpoints, see `morph.rpc_endpoint` section"))
	}
	return es
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

// SwitchInterval returns the value of "switch_interval" config parameter
// from "morph" section.
//
// Returns SwitchIntervalDefault if value is not positive duration.
func SwitchInterval(c *config.Config) time.Duration {
	res := config.DurationSafe(c.Sub(subsection), "switch_interval")
	if res != 0 {
		return res
	}

	return SwitchIntervalDefault
}
