package util

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	airportsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/airports"
	locodebolt "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/boltdb"
	continentsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/continents/geojson"
	csvlocode "github.com/nspcc-dev/neofs-node/pkg/util/locode/table/csv"
	"github.com/spf13/cobra"
)

type namesDB struct {
	*airportsdb.DB
	*csvlocode.Table
}

const (
	locodeGenerateInputFlag      = "in"
	locodeGenerateSubDivFlag     = "subdiv"
	locodeGenerateAirportsFlag   = "airports"
	locodeGenerateCountriesFlag  = "countries"
	locodeGenerateContinentsFlag = "continents"
	locodeGenerateOutputFlag     = "out"
)

var (
	locodeGenerateInPaths        []string
	locodeGenerateSubDivPath     string
	locodeGenerateAirportsPath   string
	locodeGenerateCountriesPath  string
	locodeGenerateContinentsPath string
	locodeGenerateOutPath        string

	locodeGenerateCmd = &cobra.Command{
		Use:   "generate",
		Short: "generate UN/LOCODE database for NeoFS",
		Run: func(cmd *cobra.Command, _ []string) {
			locodeDB := csvlocode.New(
				csvlocode.Prm{
					Path:       locodeGenerateInPaths[0],
					SubDivPath: locodeGenerateSubDivPath,
				},
				csvlocode.WithExtraPaths(locodeGenerateInPaths[1:]...),
			)

			airportDB := airportsdb.New(airportsdb.Prm{
				AirportsPath:  locodeGenerateAirportsPath,
				CountriesPath: locodeGenerateCountriesPath,
			})

			continentsDB := continentsdb.New(continentsdb.Prm{
				Path: locodeGenerateContinentsPath,
			})

			targetDB := locodebolt.New(locodebolt.Prm{
				Path: locodeGenerateOutPath,
			})

			err := targetDB.Open()
			common.ExitOnErr(cmd, "", err)

			defer targetDB.Close()

			names := &namesDB{
				DB:    airportDB,
				Table: locodeDB,
			}

			err = locodedb.FillDatabase(locodeDB, airportDB, continentsDB, names, targetDB)
			common.ExitOnErr(cmd, "", err)
		},
	}
)

func initUtilLocodeGenerateCmd() {
	flags := locodeGenerateCmd.Flags()

	flags.StringSliceVar(&locodeGenerateInPaths, locodeGenerateInputFlag, nil, "List of paths to UN/LOCODE tables (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateInputFlag)

	flags.StringVar(&locodeGenerateSubDivPath, locodeGenerateSubDivFlag, "", "Path to UN/LOCODE subdivision database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateSubDivFlag)

	flags.StringVar(&locodeGenerateAirportsPath, locodeGenerateAirportsFlag, "", "Path to OpenFlights airport database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateAirportsFlag)

	flags.StringVar(&locodeGenerateCountriesPath, locodeGenerateCountriesFlag, "", "Path to OpenFlights country database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateCountriesFlag)

	flags.StringVar(&locodeGenerateContinentsPath, locodeGenerateContinentsFlag, "", "Path to continent polygons (GeoJSON)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateContinentsFlag)

	flags.StringVar(&locodeGenerateOutPath, locodeGenerateOutputFlag, "", "Target path for generated database")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateOutputFlag)
}
