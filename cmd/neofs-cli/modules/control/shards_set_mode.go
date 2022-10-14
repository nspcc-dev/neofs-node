package control

import (
	"fmt"
	"strings"

	"github.com/mr-tron/base58"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
// enough to add the map entry.
var mShardModes = map[string]control.ShardMode{
	"read-only":           control.ShardMode_READ_ONLY,
	"read-write":          control.ShardMode_READ_WRITE,
	"degraded-read-write": control.ShardMode_DEGRADED,
	"degraded-read-only":  control.ShardMode_DEGRADED_READ_ONLY,
}

var setShardModeCmd = &cobra.Command{
	Use:   "set-mode",
	Short: "Set work mode of the shard",
	Long:  "Set work mode of the shard",
	Run:   setShardMode,
}

func initControlSetShardModeCmd() {
	initControlFlags(setShardModeCmd)

	flags := setShardModeCmd.Flags()
	flags.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	flags.Bool(shardAllFlag, false, "Process all shards")

	modes := make([]string, 0, len(mShardModes))
	for strMode := range mShardModes {
		modes = append(modes, "'"+strMode+"'")
	}

	flags.String(shardModeFlag, "",
		fmt.Sprintf("New shard mode keyword (%s)", strings.Join(modes, ",")),
	)
	flags.Bool(shardClearErrorsFlag, false, "Set shard error count to 0")

	setShardModeCmd.MarkFlagsMutuallyExclusive(shardIDFlag, shardAllFlag)
}

func setShardMode(cmd *cobra.Command, _ []string) {
	pk := key.Get(cmd)

	strMode, _ := cmd.Flags().GetString(shardModeFlag)

	mode, ok := mShardModes[strMode]
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

	cli := getClient(cmd, pk)

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
	if len(sidList) == 0 {
		common.ExitOnErr(cmd, "", fmt.Errorf("either --%s or --%s flag must be provided", shardIDFlag, shardAllFlag))
	}

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
