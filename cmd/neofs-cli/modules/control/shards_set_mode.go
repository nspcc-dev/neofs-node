package control

import (
	"fmt"

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

	shardModeReadOnly         = "read-only"
	shardModeReadWrite        = "read-write"
	shardModeDegraded         = "degraded-read-write"
	shardModeDegradedReadOnly = "degraded-read-only"
)

var setShardModeCmd = &cobra.Command{
	Use:   "set-mode",
	Short: "Set work mode of the shard",
	Long:  "Set work mode of the shard",
	Run:   setShardMode,
}

func initControlSetShardModeCmd() {
	commonflags.InitWithoutRPC(setShardModeCmd)

	flags := setShardModeCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	flags.Bool(shardAllFlag, false, "Process all shards")
	flags.String(shardModeFlag, "",
		fmt.Sprintf("New shard mode keyword ('%s', '%s', '%s')",
			shardModeReadWrite,
			shardModeReadOnly,
			shardModeDegraded,
		),
	)
	flags.Bool(shardClearErrorsFlag, false, "Set shard error count to 0")

	setShardModeCmd.MarkFlagsMutuallyExclusive(shardIDFlag, shardAllFlag)
}

func setShardMode(cmd *cobra.Command, _ []string) {
	pk := key.Get(cmd)

	var mode control.ShardMode

	switch shardMode, _ := cmd.Flags().GetString(shardModeFlag); shardMode {
	default:
		common.ExitOnErr(cmd, "", fmt.Errorf("unsupported mode %s", shardMode))
	case shardModeReadWrite:
		mode = control.ShardMode_READ_WRITE
	case shardModeReadOnly:
		mode = control.ShardMode_READ_ONLY
	case shardModeDegraded:
		mode = control.ShardMode_DEGRADED
	case shardModeDegradedReadOnly:
		mode = control.ShardMode_DEGRADED_READ_ONLY
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
