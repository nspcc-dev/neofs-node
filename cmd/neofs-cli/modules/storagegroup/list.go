package storagegroup

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

var sgListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage groups in NeoFS container",
	Long:  "List storage groups in NeoFS container",
	Args:  cobra.NoArgs,
	Run:   listSG,
}

func initSGListCmd() {
	commonflags.Init(sgListCmd)

	sgListCmd.Flags().String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgListCmd.MarkFlagRequired(commonflags.CIDFlag)
}

func listSG(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	readCID(cmd, &cnr)

	pk := key.GetOrGenerate(cmd)

	cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

	var prm internalclient.SearchObjectsPrm
	objectCli.Prepare(cmd, &prm)
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)
	prm.SetContainerID(cnr)
	prm.SetFilters(storagegroup.SearchQuery())

	res, err := internalclient.SearchObjects(ctx, prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	ids := res.IDList()

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for i := range ids {
		cmd.Println(ids[i].String())
	}
}
