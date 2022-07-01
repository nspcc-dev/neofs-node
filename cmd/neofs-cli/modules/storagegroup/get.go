package storagegroup

import (
	"bytes"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
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
	Run:   getSG,
}

func initSGGetCmd() {
	commonflags.Init(sgGetCmd)

	flags := sgGetCmd.Flags()

	flags.String(cidFlag, "", "Container ID")
	_ = sgGetCmd.MarkFlagRequired(cidFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)

	flags.Bool(sgRawFlag, false, "Set raw request option")
}

func getSG(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	addr := readObjectAddress(cmd, &cnr, &obj)
	pk := key.GetOrGenerate(cmd)
	buf := bytes.NewBuffer(nil)

	var prm internalclient.GetObjectPrm
	sessionCli.Prepare(cmd, cnr, &obj, pk, &prm)
	objectCli.Prepare(cmd, &prm)

	raw, _ := cmd.Flags().GetBool(sgRawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	res, err := internalclient.GetObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	rawObj := res.Header()
	rawObj.SetPayload(buf.Bytes())

	var sg storagegroupSDK.StorageGroup

	err = storagegroupSDK.ReadFromObject(&sg, *rawObj)
	common.ExitOnErr(cmd, "could not read storage group from the obj: %w", err)

	cmd.Printf("Expiration epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	common.PrintChecksum(cmd, "Group hash", sg.ValidationDataHash)

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i].String())
		}
	}
}
