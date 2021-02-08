package cmd

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/util/keyer"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	airportsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/airports"
	locodebolt "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/boltdb"
	continentsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/continents/geojson"
	csvlocode "github.com/nspcc-dev/neofs-node/pkg/util/locode/table/csv"
	"github.com/spf13/cobra"
)

var errKeyerSingleArgument = errors.New("pass only one argument at a time")

var (
	utilCmd = &cobra.Command{
		Use:   "util",
		Short: "Utility operations",
	}

	signCmd = &cobra.Command{
		Use:   "sign",
		Short: "sign NeoFS structure",
	}

	signBearerCmd = &cobra.Command{
		Use:   "bearer-token",
		Short: "sign bearer token to use it in requests",
		RunE:  signBearerToken,
	}

	convertCmd = &cobra.Command{
		Use:   "convert",
		Short: "convert representation of NeoFS structures",
	}

	convertEACLCmd = &cobra.Command{
		Use:   "eacl",
		Short: "convert representation of extended ACL table",
		RunE:  convertEACLTable,
	}

	keyerCmd = &cobra.Command{
		Use:   "keyer",
		Short: "generate or print information about keys",
		RunE:  processKeyer,
	}
)

// locode section
var locodeCmd = &cobra.Command{
	Use:   "locode",
	Short: "Working with NeoFS UN/LOCODE database",
}

const (
	locodeGenerateInputFlag      = "in"
	locodeGenerateSubDivFlag     = "subdiv"
	locodeGenerateAirportsFlag   = "airports"
	locodeGenerateCountriesFlag  = "countries"
	locodeGenerateContinentsFlag = "continents"
	locodeGenerateOutputFlag     = "out"
)

type namesDB struct {
	*airportsdb.DB
	*csvlocode.Table
}

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
		RunE: func(cmd *cobra.Command, _ []string) error {
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
			if err != nil {
				return err
			}

			defer targetDB.Close()

			names := &namesDB{
				DB:    airportDB,
				Table: locodeDB,
			}

			err = locodedb.FillDatabase(locodeDB, airportDB, continentsDB, names, targetDB)
			if err != nil {
				return err
			}

			return nil
		},
	}
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
		Short: "print information about UN/LOCODE from NeoFS database",
		RunE: func(cmd *cobra.Command, _ []string) error {
			targetDB := locodebolt.New(locodebolt.Prm{
				Path: locodeInfoDBPath,
			})

			err := targetDB.Open()
			if err != nil {
				return err
			}

			record, err := locodedb.LocodeRecord(targetDB, locodeInfoCode)
			if err != nil {
				return err
			}

			fmt.Printf("Country: %s\n", record.CountryName())
			fmt.Printf("City: %s\n", record.CityName())
			fmt.Printf("Continent: %s\n", record.Continent())
			if subDivCode := record.SubDivCode(); subDivCode != "" {
				fmt.Printf("Subdivision: [%s] %s\n", subDivCode, record.SubDivName())
			}

			geoPoint := record.GeoPoint()
			fmt.Printf("Coordinates: %0.2f, %0.2f\n", geoPoint.Latitude(), geoPoint.Longitude())

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(utilCmd)

	utilCmd.AddCommand(signCmd)
	utilCmd.AddCommand(convertCmd)
	utilCmd.AddCommand(keyerCmd)
	utilCmd.AddCommand(locodeCmd)

	signCmd.AddCommand(signBearerCmd)
	signBearerCmd.Flags().String("from", "", "File with JSON or binary encoded bearer token to sign")
	_ = signBearerCmd.MarkFlagFilename("from")
	_ = signBearerCmd.MarkFlagRequired("from")
	signBearerCmd.Flags().String("to", "", "File to dump signed bearer token (default: binary encoded)")
	signBearerCmd.Flags().Bool("json", false, "Dump bearer token in JSON encoding")

	convertCmd.AddCommand(convertEACLCmd)
	convertEACLCmd.Flags().String("from", "", "File with JSON or binary encoded extended ACL table")
	_ = convertEACLCmd.MarkFlagFilename("from")
	_ = convertEACLCmd.MarkFlagRequired("from")
	convertEACLCmd.Flags().String("to", "", "File to dump extended ACL table (default: binary encoded)")
	convertEACLCmd.Flags().Bool("json", false, "Dump extended ACL table in JSON encoding")

	keyerCmd.Flags().BoolP("generate", "g", false, "generate new private key")
	keyerCmd.Flags().Bool("hex", false, "print all values in hex encoding")
	keyerCmd.Flags().BoolP("uncompressed", "u", false, "use uncompressed public key format")
	keyerCmd.Flags().BoolP("multisig", "m", false, "calculate multisig address from public keys")

	locodeCmd.AddCommand(locodeGenerateCmd)

	locodeGenerateCmd.Flags().StringSliceVar(&locodeGenerateInPaths, locodeGenerateInputFlag, nil,
		"List of paths to UN/LOCODE tables (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateInputFlag)

	locodeGenerateCmd.Flags().StringVar(&locodeGenerateSubDivPath, locodeGenerateSubDivFlag, "",
		"Path to UN/LOCODE subdivision database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateSubDivFlag)

	locodeGenerateCmd.Flags().StringVar(&locodeGenerateAirportsPath, locodeGenerateAirportsFlag, "",
		"Path to OpenFlights airport database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateAirportsFlag)

	locodeGenerateCmd.Flags().StringVar(&locodeGenerateCountriesPath, locodeGenerateCountriesFlag, "",
		"Path to OpenFlights country database (csv)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateCountriesFlag)

	locodeGenerateCmd.Flags().StringVar(&locodeGenerateContinentsPath, locodeGenerateContinentsFlag, "",
		"Path to continent polygons (GeoJSON)")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateContinentsFlag)

	locodeGenerateCmd.Flags().StringVar(&locodeGenerateOutPath, locodeGenerateOutputFlag, "",
		"Target path for generated database")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeGenerateOutputFlag)

	locodeCmd.AddCommand(locodeInfoCmd)

	locodeInfoCmd.Flags().StringVar(&locodeInfoDBPath, locodeInfoDBFlag, "",
		"Path to NeoFS UN/LOCODE database")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeInfoDBFlag)

	locodeInfoCmd.Flags().StringVar(&locodeInfoCode, locodeInfoCodeFlag, "",
		"UN/LOCODE")
	_ = locodeGenerateCmd.MarkFlagRequired(locodeInfoCodeFlag)
}

func signBearerToken(cmd *cobra.Command, _ []string) error {
	btok, err := getBearerToken(cmd, "from")
	if err != nil {
		return err
	}

	key, err := getKey()
	if err != nil {
		return err
	}

	err = completeBearerToken(btok)
	if err != nil {
		return err
	}

	err = btok.SignToken(key)
	if err != nil {
		return err
	}

	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool("json")

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = btok.MarshalJSON()
		if err != nil {
			return fmt.Errorf("can't JSON encode bearer token: %w", err)
		}
	} else {
		data, err = btok.Marshal()
		if err != nil {
			return fmt.Errorf("can't binary encode bearer token: %w", err)
		}
	}

	if len(to) == 0 {
		prettyPrintJSON(cmd, data)

		return nil
	}

	err = ioutil.WriteFile(to, data, 0644)
	if err != nil {
		return fmt.Errorf("can't write signed bearer token to file: %w", err)
	}

	cmd.Printf("signed bearer token was successfully dumped to %s\n", to)

	return nil
}

func convertEACLTable(cmd *cobra.Command, _ []string) error {
	pathFrom := cmd.Flag("from").Value.String()
	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool("json")

	table, err := parseEACL(pathFrom)
	if err != nil {
		return err
	}

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = table.MarshalJSON()
		if err != nil {
			return fmt.Errorf("can't JSON encode extended ACL table: %w", err)
		}
	} else {
		data, err = table.Marshal()
		if err != nil {
			return fmt.Errorf("can't binary encode extended ACL table: %w", err)
		}
	}

	if len(to) == 0 {
		prettyPrintJSON(cmd, data)

		return nil
	}

	err = ioutil.WriteFile(to, data, 0644)
	if err != nil {
		return fmt.Errorf("can't write exteded ACL table to file: %w", err)
	}

	cmd.Printf("extended ACL table was successfully dumped to %s\n", to)

	return nil
}

func processKeyer(cmd *cobra.Command, args []string) error {
	var (
		err error

		result          = new(keyer.Dashboard)
		generate, _     = cmd.Flags().GetBool("generate")
		useHex, _       = cmd.Flags().GetBool("hex")
		uncompressed, _ = cmd.Flags().GetBool("uncompressed")
		multisig, _     = cmd.Flags().GetBool("multisig")
	)

	if multisig {
		err = result.ParseMultiSig(args)
	} else {
		if len(args) > 1 {
			return errKeyerSingleArgument
		}

		var argument string
		if len(args) > 0 {
			argument = args[0]
		}

		switch {
		case generate:
			err = keyerGenerate(argument, result)
		case fileExists(argument):
			err = keyerParseFile(argument, result)
		default:
			err = result.ParseString(argument)
		}
	}

	if err != nil {
		return err
	}

	result.PrettyPrint(uncompressed, useHex)

	return nil
}

func completeBearerToken(btok *token.BearerToken) error {
	if v2 := btok.ToV2(); v2 != nil {
		// set eACL table version, because it usually omitted
		table := v2.GetBody().GetEACL()
		table.SetVersion(pkg.SDKVersion().ToV2())
	} else {
		return errors.New("unsupported bearer token version")
	}

	return nil
}

func prettyPrintJSON(cmd *cobra.Command, data []byte) {
	buf := new(bytes.Buffer)
	if err := json.Indent(buf, data, "", "  "); err != nil {
		printVerbose("Can't pretty print json: %w", err)
	}

	cmd.Println(buf)
}

func prettyPrintUnixTime(s string) string {
	unixTime, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "malformed"
	}

	timestamp := time.Unix(unixTime, 0)

	return timestamp.String()
}

func keyerGenerate(filename string, d *keyer.Dashboard) error {
	key := make([]byte, keyer.NeoPrivateKeySize)

	_, err := rand.Read(key)
	if err != nil {
		return fmt.Errorf("can't get random source: %w", err)
	}

	err = d.ParseBinary(key)
	if err != nil {
		return fmt.Errorf("can't parse key: %w", err)
	}

	if filename != "" {
		return ioutil.WriteFile(filename, key, 0600)
	}

	return nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func keyerParseFile(filename string, d *keyer.Dashboard) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't open %v file: %w", filename, err)
	}

	return d.ParseBinary(data)
}
