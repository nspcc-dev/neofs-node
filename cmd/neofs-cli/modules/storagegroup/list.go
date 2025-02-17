package storagegroup

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	bt, err := common.ReadBearerToken(cmd, objectCli.BearerTokenFlag)
	if err != nil {
		return err
	}
	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}

	var opts client.SearchObjectsOptions
	ttl := viper.GetUint32(commonflags.TTL)
	common.PrintVerbose(cmd, "TTL: %d", ttl)
	if ttl == 1 {
		opts.DisableForwarding()
	}
	if bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(objectCli.ParseXHeaders(cmd)...)

	fs := storagegroup.SearchQuery()
	var sets [][]client.SearchResultItem
	var next []client.SearchResultItem
	var cursor string
	var n int
	for {
		next, cursor, err = cli.SearchObjects(ctx, cnr, fs, nil, cursor, (*neofsecdsa.Signer)(pk), opts)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
		sets = append(sets, next)
		n += len(next)
		if cursor == "" {
			break
		}
	}

	cmd.Printf("Found %d storage groups.\n", n)

	for i := range sets {
		for j := range sets[i] {
			cmd.Println(sets[i][j].ID)
		}
	}

	return nil
}
