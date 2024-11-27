package fschainconfig

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection                = "fschain"
	deprecatedMorphSubsection = "morph"

	// DialTimeoutDefault is a default dial timeout of FS chain client connection.
	DialTimeoutDefault = time.Minute

	// CacheTTLDefault is a default value for cached values TTL.
	// It is 0, because actual default depends on block time.
	CacheTTLDefault = time.Duration(0)

	// ReconnectionRetriesNumberDefault is a default value for reconnection retries.
	ReconnectionRetriesNumberDefault = 5
	// ReconnectionRetriesDelayDefault is a default delay b/w reconnections.
	ReconnectionRetriesDelayDefault = 5 * time.Second
)

// Endpoints returns list of the values of "endpoints" config parameter
// from "fschain" section (primary) or from "morph" section.
//
// Throws panic if list is empty.
func Endpoints(c *config.Config) []string {
	endpoints := config.StringSliceSafe(c.Sub(subsection), "endpoints")
	morphEndpoints := config.StringSliceSafe(c.Sub(deprecatedMorphSubsection), "endpoints")

	if len(endpoints) == 0 && len(morphEndpoints) == 0 {
		panic(fmt.Errorf("no FS chain RPC endpoints, see `fschain.endpoints` section"))
	}
	if len(endpoints) > 0 {
		return endpoints
	}
	return morphEndpoints
}

// DialTimeout returns the value of "dial_timeout" config parameter
// from "fschain" section (primary) or from "morph" section.
//
// Returns DialTimeoutDefault if the value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	morphV := config.DurationSafe(c.Sub(deprecatedMorphSubsection), "dial_timeout")

	if v > 0 {
		return v
	}
	if morphV > 0 {
		return morphV
	}

	return DialTimeoutDefault
}

// CacheTTL returns the value of "cache_ttl" config parameter
// from "fschain" section (primary) or from "morph" section.
//
// Returns CacheTTLDefault if value is zero or invalid. Supports negative durations.
func CacheTTL(c *config.Config) time.Duration {
	res := config.DurationSafe(c.Sub(subsection), "cache_ttl")
	morphRes := config.DurationSafe(c.Sub(deprecatedMorphSubsection), "cache_ttl")

	if res != 0 {
		return res
	}
	if morphRes != 0 {
		return morphRes
	}

	return CacheTTLDefault
}

// ReconnectionRetriesNumber returns the value of "reconnections_number" config
// parameter from "fschain" section (primary) or from "morph" section.
//
// Returns 0 if value is not specified.
func ReconnectionRetriesNumber(c *config.Config) int {
	res := config.Int(c.Sub(subsection), "reconnections_number")
	morphRes := config.Int(c.Sub(deprecatedMorphSubsection), "reconnections_number")

	if res != 0 {
		return int(res)
	}
	if morphRes != 0 {
		return int(morphRes)
	}

	return ReconnectionRetriesNumberDefault
}

// ReconnectionRetriesDelay returns the value of "reconnections_delay" config
// parameter from "fschain" section (primary) or from "morph" section.
//
// Returns 0 if value is not specified.
func ReconnectionRetriesDelay(c *config.Config) time.Duration {
	res := config.DurationSafe(c.Sub(subsection), "reconnections_delay")
	morphRes := config.DurationSafe(c.Sub(deprecatedMorphSubsection), "reconnections_delay")

	if res != 0 {
		return res
	}
	if morphRes != 0 {
		return morphRes
	}

	return ReconnectionRetriesDelayDefault
}
