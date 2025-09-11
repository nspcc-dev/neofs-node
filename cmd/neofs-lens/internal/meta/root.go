package meta

import (
	"fmt"
	"os"
	"time"

	"github.com/nspcc-dev/bbolt"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
)

var (
	vAddress  string
	vPath     string
	vInputObj string
)

type epochState struct{}

func (s epochState) CurrentEpoch() uint64 {
	return 0
}

// Root contains `meta` command definition.
var Root = &cobra.Command{
	Use:   "meta",
	Short: "Operations with a metabase",
}

func init() {
	Root.AddCommand(
		listCMD,
		listGraveyardCMD,
		listGarbageCMD,
		writeObjectCMD,
		getCMD,
		statCMD,
		lastResyncEpochCMD,
	)
}

func openMeta(readOnly bool) (*meta.DB, error) {
	_, err := os.Stat(vPath)
	if err != nil {
		return nil, err
	}

	db := meta.New(
		meta.WithPath(vPath),
		meta.WithBoltDBOptions(&bbolt.Options{
			ReadOnly: readOnly,
			Timeout:  time.Second,
		}),
		meta.WithEpochState(epochState{}),
	)
	if err := db.Open(readOnly); err != nil {
		return nil, fmt.Errorf("could not open metabase: %w", err)
	}

	return db, nil
}
