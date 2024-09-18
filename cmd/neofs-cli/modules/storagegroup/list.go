package storagegroup

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
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
	RunE:  listSG,
}

func initSGListCmd() {
	commonflags.Init(sgListCmd)

	sgListCmd.Flags().String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgListCmd.MarkFlagRequired(commonflags.CIDFlag)
}

func listSG(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	err := readCID(cmd, &cnr)
	if err != nil {
		return err
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}

	var prm internalclient.SearchObjectsPrm
	err = objectCli.Prepare(cmd, &prm)
	if err != nil {
		return err
	}
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)
	prm.SetContainerID(cnr)
	prm.SetFilters(storagegroup.SearchQuery())

	res, err := internalclient.SearchObjects(ctx, prm)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	ids := res.IDList()

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for i := range ids {
		cmd.Println(ids[i].String())
	}

	return nil
}
