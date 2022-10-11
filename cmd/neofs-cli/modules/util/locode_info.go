package util

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	locodebolt "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/boltdb"
	"github.com/spf13/cobra"
)

const (
	locodeInfoDBFlag   = "db"
	locodeInfoCodeFlag = "locode"
)

var (
	locodeInfoDBPath string
	locodeInfoCode   string

	locodeInfoCmd = &cobra.Command{
		Use:   "info",
		Short: "Print information about UN/LOCODE from NeoFS database",
		Run: func(cmd *cobra.Command, _ []string) {
			targetDB := locodebolt.New(locodebolt.Prm{
				Path: locodeInfoDBPath,
			}, locodebolt.ReadOnly())

			err := targetDB.Open()
			common.ExitOnErr(cmd, "", err)

			defer targetDB.Close()

			record, err := locodedb.LocodeRecord(targetDB, locodeInfoCode)
			common.ExitOnErr(cmd, "", err)

			cmd.Printf("Country: %s\n", record.CountryName())
			cmd.Printf("Location: %s\n", record.LocationName())
			cmd.Printf("Continent: %s\n", record.Continent())
			if subDivCode := record.SubDivCode(); subDivCode != "" {
				cmd.Printf("Subdivision: [%s] %s\n", subDivCode, record.SubDivName())
			}

			geoPoint := record.GeoPoint()
			cmd.Printf("Coordinates: %0.2f, %0.2f\n", geoPoint.Latitude(), geoPoint.Longitude())
		},
	}
)

func initUtilLocodeInfoCmd() {
	flags := locodeInfoCmd.Flags()

	flags.StringVar(&locodeInfoDBPath, locodeInfoDBFlag, "", "Path to NeoFS UN/LOCODE database")
	_ = locodeInfoCmd.MarkFlagRequired(locodeInfoDBFlag)

	flags.StringVar(&locodeInfoCode, locodeInfoCodeFlag, "", "UN/LOCODE")
	_ = locodeInfoCmd.MarkFlagRequired(locodeInfoCodeFlag)
}
