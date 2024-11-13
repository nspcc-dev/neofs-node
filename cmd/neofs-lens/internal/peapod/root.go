package peapod

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/spf13/cobra"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
)

// Root defines root command for operations with Peapod.
var Root = &cobra.Command{
	Use:   "peapod",
	Short: "Operations with a Peapod",
}

func init() {
	Root.AddCommand(listCMD)
	Root.AddCommand(getCMD)
}

// open and returns read-only peapod.Peapod located in vPath.
func openPeapod() (
	// nolint:staticcheck
	*peapod.Peapod,
	error,
) {
	// interval prm doesn't matter for read-only usage, but must be positive
	ppd := peapod.New(vPath, 0400, 1)
	var compressCfg compression.Config

	err := compressCfg.Init()
	if err != nil {
		return nil, fmt.Errorf("failed to init compression config: %w", err)
	}

	ppd.SetCompressor(&compressCfg)

	err = ppd.Open(true)
	if err != nil {
		return nil, fmt.Errorf("failed to open Peapod: %w", err)
	}

	return ppd, nil
}
