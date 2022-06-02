package storagegroup

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

func readObjectAddress(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID) oid.Address {
	readCID(cmd, cnr)
	readSGID(cmd, obj)

	var addr oid.Address
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr
}

func readCID(cmd *cobra.Command, id *cid.ID) {
	err := id.DecodeString(cmd.Flag("cid").Value.String())
	common.ExitOnErr(cmd, "decode container ID string: %w", err)
}

func readSGID(cmd *cobra.Command, id *oid.ID) {
	err := id.DecodeString(cmd.Flag("oid").Value.String())
	common.ExitOnErr(cmd, "decode storage group ID string: %w", err)
}
