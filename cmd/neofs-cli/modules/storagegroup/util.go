package storagegroup

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
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
	f := cmd.Flag(commonflags.CIDFlag)
	if f == nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("missing container flag (%s)", commonflags.CIDFlag))
		return
	}

	err := id.DecodeString(f.Value.String())
	common.ExitOnErr(cmd, "decode container ID string: %w", err)
}

func readSGID(cmd *cobra.Command, id *oid.ID) {
	const flag = "id"

	f := cmd.Flag(flag)
	if f == nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("missing storage group flag (%s)", flag))
		return
	}

	err := id.DecodeString(f.Value.String())
	common.ExitOnErr(cmd, "decode storage group ID string: %w", err)
}
