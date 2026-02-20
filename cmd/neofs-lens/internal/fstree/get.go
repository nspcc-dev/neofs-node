package fstree

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Object inspection",
	Long:  `Get specific object from FSTree.`,
	Args:  cobra.NoArgs,
	RunE:  getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
	common.AddOutputFileFlag(getCMD, &vOut)
	common.AddPayloadOnlyFlag(getCMD, &vPayloadOnly)
	AddDepthFlag(getCMD, &vDepth)
}

func getFunc(cmd *cobra.Command, _ []string) error {
	fst, err := openFSTree()
	if err != nil {
		return err
	}
	defer fst.Close()

	addr, err := oid.DecodeAddressString(vAddress)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	obj, err := fst.Get(addr)
	if err != nil {
		return fmt.Errorf("could not fetch object: %w", err)
	}

	common.PrintObjectHeader(cmd, *obj)

	return common.WriteObjectToFile(cmd, vOut, obj.Marshal(), vPayloadOnly)
}
