package morphconfig

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "morph"

	// DialTimeoutDefault is a default dial timeout of morph chain client connection.
	DialTimeoutDefault = 5 * time.Second
)

// RPCEndpoint returns list of values of "rpc_endpoint" config parameter
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

// NotificationEndpoint returns list of values of "notification_endpoint" config
// parameter from "morph" section.
//
// Throws panic if list is empty.
func NotificationEndpoint(c *config.Config) []string {
	v := config.StringSliceSafe(c.Sub(subsection), "notification_endpoint")
	if len(v) == 0 {
		panic(fmt.Errorf("no morph chain notification endpoints, see `morph.notification_endpoint` section"))
	}

	return v
}

// DialTimeout returns value of "dial_timeout" config parameter
// from "morph" section.
//
// Returns DialTimeoutDefault if value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	if v != 0 {
		return v
	}

	return DialTimeoutDefault
}
