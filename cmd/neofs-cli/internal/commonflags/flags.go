package commonflags

import (
	"context"
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
	generateKeyUsage     = "Generate new private key"

	WalletPath          = "wallet"
	WalletPathShorthand = "w"
	WalletPathDefault   = ""
	WalletPathUsage     = "Path to the wallet"

	Account          = "address"
	AccountShorthand = ""
	AccountDefault   = ""
	AccountUsage     = "Address of wallet account"

	RPC          = "rpc-endpoint"
	RPCShorthand = "r"
	RPCDefault   = ""
	RPCUsage     = "Remote node address (as 'multiaddr' or '<host>:<port>')"

	Timeout          = "timeout"
	TimeoutShorthand = "t"
	TimeoutDefault   = 15 * time.Second
	TimeoutUsage     = "Timeout for the operation"

	Verbose          = "verbose"
	VerboseShorthand = "v"
	VerboseUsage     = "Verbose output"

	ForceFlag          = "force"
	ForceFlagShorthand = "f"

	CIDFlag      = "cid"
	CIDFlagUsage = "Container ID."

	OIDFlag      = "oid"
	OIDFlagUsage = "Object ID."

	SessionSubjectFlag      = "session-subjects"
	SessionSubjectFlagUsage = "Session subject user IDs (optional, defaults to current node)"

	SessionSubjectNNSFlag      = "session-subjects-nns"
	SessionSubjectNNSFlagUsage = "Session subject NNS names (optional, defaults to current node)"
)

// Init adds common flags to the command:
// - GenerateKey,
// - WalletPath,
// - Account,
// - RPC,
// - Timeout.
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

// GetCommandContextWithAwait works like GetCommandContext but uses specified
// await timeout if 'timeout' flag is omitted and given boolean await flag is
// set.
func GetCommandContextWithAwait(cmd *cobra.Command, awaitFlag string, awaitTimeout time.Duration) (context.Context, context.CancelFunc) {
	if !viper.IsSet(Timeout) {
		if await, _ := cmd.Flags().GetBool(awaitFlag); await {
			return getCommandContext(cmd, awaitTimeout)
		}
	}

	return GetCommandContext(cmd)
}

// GetCommandContext returns cmd context with timeout specified in 'timeout' flag
// if the flag is set.
func GetCommandContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return getCommandContext(cmd, viper.GetDuration(Timeout))
}

func getCommandContext(cmd *cobra.Command, timeout time.Duration) (context.Context, context.CancelFunc) {
	parentCtx := cmd.Context()
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	if timeout <= 0 {
		return parentCtx, func() {}
	}

	return context.WithTimeout(parentCtx, timeout)
}
