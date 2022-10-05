package commonflags

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Common CLI flag keys, shorthands, default
// values and their usage descriptions.
const (
	GenerateKey          = "generate-key"
	generateKeyShorthand = "g"
	generateKeyDefault   = false
	generateKeyUsage     = "generate new private key"

	WalletPath          = "wallet"
	WalletPathShorthand = "w"
	WalletPathDefault   = ""
	WalletPathUsage     = "path to the wallet or binary key"

	Account          = "address"
	AccountShorthand = ""
	AccountDefault   = ""
	AccountUsage     = "address of wallet account"

	RPC          = "rpc-endpoint"
	RPCShorthand = "r"
	RPCDefault   = ""
	RPCUsage     = "remote node address (as 'multiaddr' or '<host>:<port>')"

	Timeout          = "timeout"
	TimeoutShorthand = "t"
	TimeoutDefault   = 15 * time.Second
	TimeoutUsage     = "timeout for an operation"

	Verbose          = "verbose"
	VerboseShorthand = "v"
	VerboseUsage     = "verbose output"

	ForceFlag          = "force"
	ForceFlagShorthand = "f"
)

// Init adds common flags to the command:
// - GenerateKey
// - WalletPath
// - Account
// - RPC
// - Timeout
func Init(cmd *cobra.Command) {
	InitWithoutRPC(cmd)

	ff := cmd.Flags()
	ff.StringP(RPC, RPCShorthand, RPCDefault, RPCUsage)
	ff.DurationP(Timeout, TimeoutShorthand, TimeoutDefault, TimeoutUsage)
}

// InitWithoutRPC is similar to Init but doesn't create the RPC flag.
func InitWithoutRPC(cmd *cobra.Command) {
	ff := cmd.Flags()

	ff.BoolP(GenerateKey, generateKeyShorthand, generateKeyDefault, generateKeyUsage)
	ff.StringP(WalletPath, WalletPathShorthand, WalletPathDefault, WalletPathUsage)
	ff.StringP(Account, AccountShorthand, AccountDefault, AccountUsage)
}

// Bind binds common command flags to the viper.
func Bind(cmd *cobra.Command) {
	ff := cmd.Flags()

	_ = viper.BindPFlag(GenerateKey, ff.Lookup(GenerateKey))
	_ = viper.BindPFlag(WalletPath, ff.Lookup(WalletPath))
	_ = viper.BindPFlag(Account, ff.Lookup(Account))
	_ = viper.BindPFlag(RPC, ff.Lookup(RPC))
	_ = viper.BindPFlag(Timeout, ff.Lookup(Timeout))
}
