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
	storagegroupAPI "github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/spf13/cobra"
)

const (
	sgIDFlag  = "id"
	sgRawFlag = "raw"
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

	flags.String("cid", "", "Container ID")
	_ = sgGetCmd.MarkFlagRequired("cid")

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

	_, err := internalclient.GetObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	var sg storagegroupAPI.StorageGroup

	err = sg.Unmarshal(buf.Bytes())
	common.ExitOnErr(cmd, "could not unmarshal storage group: %w", err)

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
