package storagegroup

import (
	"bytes"
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	storagegroupSDK "github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/spf13/cobra"
)

var sgID string

var sgGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get storage group from NeoFS",
	Long:  "Get storage group from NeoFS",
	Args:  cobra.NoArgs,
	RunE:  getSG,
}

func initSGGetCmd() {
	commonflags.Init(sgGetCmd)

	flags := sgGetCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgGetCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "Storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)

	flags.Bool(sgRawFlag, false, "Set raw request option")
}

func getSG(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	var obj oid.ID

	addr, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}
	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}

	var prm internalclient.GetObjectPrm
	err = objectCli.Prepare(cmd, &prm)
	if err != nil {
		return err
	}
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)

	raw, _ := cmd.Flags().GetBool(sgRawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	res, err := internalclient.GetObject(ctx, prm)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	rawObj := res.Header()
	rawObj.SetPayload(buf.Bytes())

	var sg storagegroupSDK.StorageGroup

	err = storagegroupSDK.ReadFromObject(&sg, *rawObj)
	if err != nil {
		return fmt.Errorf("could not read storage group from the obj: %w", err)
	}

	expiration, err := object.Expiration(*rawObj)
	if err != nil && !errors.Is(err, object.ErrNoExpiration) {
		return fmt.Errorf("storage group's expiration: %w", err)
	}

	if errors.Is(err, object.ErrNoExpiration) {
		cmd.Printf("No expiration epoch")
	} else {
		cmd.Printf("The last active epoch: %d\n", expiration)
	}
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	common.PrintChecksum(cmd, "Group hash", sg.ValidationDataHash)

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i].String())
		}
	}

	return nil
}
