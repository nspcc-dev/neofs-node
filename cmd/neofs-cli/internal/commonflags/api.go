package commonflags

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	TTL          = "ttl"
	TTLShorthand = ""
	TTLDefault   = 2
	TTLUsage     = "TTL value in request meta header"

	XHeadersKey       = "xhdr"
	XHeadersShorthand = "x"
	XHeadersUsage     = "Request X-Headers in form of Key=Value"
)

// InitAPI inits common flags for storage node services.
func InitAPI(cmd *cobra.Command) {
	ff := cmd.Flags()

	ff.StringSliceP(XHeadersKey, XHeadersShorthand, []string{}, XHeadersUsage)
	ff.Uint32P(TTL, TTLShorthand, TTLDefault, TTLUsage)
}

// BindAPI binds API flags of storage node services to the viper.
func BindAPI(cmd *cobra.Command) {
	ff := cmd.Flags()

	_ = viper.BindPFlag(TTL, ff.Lookup(TTL))
	_ = viper.BindPFlag(XHeadersKey, ff.Lookup(XHeadersKey))
}
