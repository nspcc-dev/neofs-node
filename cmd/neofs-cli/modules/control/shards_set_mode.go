package control

import (
	"fmt"
	"strings"

	"github.com/mr-tron/base58"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

const (
	shardModeFlag        = "mode"
	shardIDFlag          = "id"
	shardAllFlag         = "all"
	shardClearErrorsFlag = "clear-errors"
)

// maps string command input to control.ShardMode. To support new mode, it's
// enough to add the map entry. Modes are automatically printed in command help
// messages.
var mShardModes = map[string]struct {
	val control.ShardMode

	// flag to support shard mode implicitly without help message. The flag is set
	// for values which are not expected to be set by users but still supported
	// for developers.
	unsafe bool
}{
	"read-only":           {val: control.ShardMode_READ_ONLY},
	"read-write":          {val: control.ShardMode_READ_WRITE},
	"degraded-read-write": {val: control.ShardMode_DEGRADED, unsafe: true},
	"degraded-read-only":  {val: control.ShardMode_DEGRADED_READ_ONLY},
}

// iterates over string representations of safe supported shard modes. Safe means
// modes which are expected to be used by any user. All other supported modes
// are for developers only.
func iterateSafeShardModes(f func(string)) {
	for strMode, mode := range mShardModes {
		if !mode.unsafe {
			f(strMode)
		}
	}
}

// looks up for supported control.ShardMode represented by the given string.
// Returns false if no corresponding mode exists.
func lookUpShardModeFromString(str string) (control.ShardMode, bool) {
	mode, ok := mShardModes[str]
	if !ok {
		return control.ShardMode_SHARD_MODE_UNDEFINED, false
	}

	return mode.val, true
}

// looks up for string representation of supported shard mode. Returns false
// if mode is not supported.
func lookUpShardModeString(m control.ShardMode) (string, bool) {
	for strMode, mode := range mShardModes {
		if mode.val == m {
			return strMode, true
		}
	}

	return "", false
}

var setShardModeCmd = &cobra.Command{
	Use:   "set-mode",
	Short: "Set work mode of the shard",
	Long:  "Set work mode of the shard",
	Args:  cobra.NoArgs,
	Run:   setShardMode,
}

func initControlSetShardModeCmd() {
	initControlFlags(setShardModeCmd)

	flags := setShardModeCmd.Flags()
	flags.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	flags.Bool(shardAllFlag, false, "Process all shards")

	modes := make([]string, 0)

	iterateSafeShardModes(func(strMode string) {
		modes = append(modes, "'"+strMode+"'")
	})

	flags.String(shardModeFlag, "",
		fmt.Sprintf("New shard mode (%s)", strings.Join(modes, ", ")),
	)
	flags.Bool(shardClearErrorsFlag, false, "Set shard error count to 0")

	setShardModeCmd.MarkFlagsOneRequired(shardIDFlag, shardAllFlag)
}

func setShardMode(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	strMode, _ := cmd.Flags().GetString(shardModeFlag)

	mode, ok := lookUpShardModeFromString(strMode)
	if !ok {
		common.ExitOnErr(cmd, "", fmt.Errorf("unsupported mode %s", strMode))
	}

	req := new(control.SetShardModeRequest)

	body := new(control.SetShardModeRequest_Body)
	req.SetBody(body)

	body.SetMode(mode)
	body.SetShardIDList(getShardIDList(cmd))

	reset, _ := cmd.Flags().GetBool(shardClearErrorsFlag)
	body.ClearErrorCounter(reset)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	var resp *control.SetShardModeResponse
	var err error
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.SetShardMode(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard mode update request successfully sent.")
}

func getShardID(cmd *cobra.Command) []byte {
	sid, _ := cmd.Flags().GetString(shardIDFlag)
	raw, err := base58.Decode(sid)
	common.ExitOnErr(cmd, "incorrect shard ID encoding: %w", err)
	return raw
}

func getShardIDList(cmd *cobra.Command) [][]byte {
	all, _ := cmd.Flags().GetBool(shardAllFlag)
	if all {
		return nil
	}

	sidList, _ := cmd.Flags().GetStringSlice(shardIDFlag)
	// if shardAllFlag is unset, shardIDFlag flag presence is guaranteed by
	// MarkFlagsOneRequired

	// We can sort the ID list and perform this check without additional allocations,
	// but preserving the user order is a nice thing to have.
	// Also, this is a CLI, we don't care too much about this.
	seen := make(map[string]struct{})
	for i := range sidList {
		if _, ok := seen[sidList[i]]; ok {
			common.ExitOnErr(cmd, "", fmt.Errorf("duplicated shard IDs: %s", sidList[i]))
		}
		seen[sidList[i]] = struct{}{}
	}

	res := make([][]byte, 0, len(sidList))
	for i := range sidList {
		raw, err := base58.Decode(sidList[i])
		common.ExitOnErr(cmd, "incorrect shard ID encoding: %w", err)

		res = append(res, raw)
	}

	return res
}
