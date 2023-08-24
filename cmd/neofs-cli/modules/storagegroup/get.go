package storagegroup

import (
	"bytes"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
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
	Run:   getSG,
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

func getSG(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var obj oid.ID

	addr := readObjectAddress(cmd, &cnr, &obj)
	pk := key.GetOrGenerate(cmd)
	buf := bytes.NewBuffer(nil)

	cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

	var prm internalclient.GetObjectPrm
	objectCli.Prepare(cmd, &prm)
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)

	raw, _ := cmd.Flags().GetBool(sgRawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	res, err := internalclient.GetObject(ctx, prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	rawObj := res.Header()
	rawObj.SetPayload(buf.Bytes())

	var sg storagegroupSDK.StorageGroup

	err = storagegroupSDK.ReadFromObject(&sg, *rawObj)
	common.ExitOnErr(cmd, "could not read storage group from the obj: %w", err)

	cmd.Printf("The last active epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	common.PrintChecksum(cmd, "Group hash", sg.ValidationDataHash)

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i].String())
		}
	}
}
