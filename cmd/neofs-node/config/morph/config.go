package morphconfig

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
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
	if v > 0 {
		return v
	}

	return DialTimeoutDefault
}

// DisableCache returns value of "disable_cache" config parameter
// from "morph" section.
func DisableCache(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "disable_cache")
}

// NotaryConfig is a wrapper over the config section
// which provides access to notary configurations
// of the side chain.
type NotaryConfig struct {
	cfg *config.Config
}

// Notary returns structure that provides access to "notary"
// subsection of "morph" section.
func Notary(c *config.Config) *NotaryConfig {
	return &NotaryConfig{
		cfg: c.Sub(subsection).Sub(notarySubsection),
	}
}

// Amount returns value of "deposit_amount" config parameter.
//
// Returns NotaryDepositAmountDefault if value is not positive amount.
func (n NotaryConfig) Amount() fixedn.Fixed8 {
	v := config.UintSafe(n.cfg, "deposit_amount")
	if v > 0 {
		return fixedn.Fixed8(v)
	}

	return fixedn.Fixed8(NotaryDepositAmountDefault)
}

func (n NotaryConfig) Duration() uint32 {
	v := config.Uint32Safe(n.cfg, "deposit_duration")
	if v > 0 {
		return v
	}

	return NotaryDepositDurationDefault
}
