package storagegroup

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

func readObjectAddress(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID) (oid.Address, error) {
	err := readCID(cmd, cnr)
	if err != nil {
		return oid.Address{}, err
	}
	err = readSGID(cmd, obj)
	if err != nil {
		return oid.Address{}, err
	}

	var addr oid.Address
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr, nil
}

func readCID(cmd *cobra.Command, id *cid.ID) error {
	f := cmd.Flag(commonflags.CIDFlag)
	if f == nil {
		return fmt.Errorf("missing container flag (%s)", commonflags.CIDFlag)
	}

	err := id.DecodeString(f.Value.String())
	if err != nil {
		return fmt.Errorf("decode container ID string: %w", err)
	}

	return nil
}

func readSGID(cmd *cobra.Command, id *oid.ID) error {
	const flag = "id"

	f := cmd.Flag(flag)
	if f == nil {
		return fmt.Errorf("missing storage group flag (%s)", flag)
	}

	err := id.DecodeString(f.Value.String())
	if err != nil {
		return fmt.Errorf("decode storage group ID string: %w", err)
	}

	return nil
}
