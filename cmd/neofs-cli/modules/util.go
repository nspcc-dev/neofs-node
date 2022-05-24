package cmd

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/util/keyer"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	airportsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/airports"
	locodebolt "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/boltdb"
	continentsdb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db/continents/geojson"
	csvlocode "github.com/nspcc-dev/neofs-node/pkg/util/locode/table/csv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errKeyerSingleArgument = errors.New("pass only one argument at a time")

var (
	utilCmd = &cobra.Command{
		Use:   "util",
		Short: "Utility operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			flags := cmd.Flags()

			_ = viper.BindPFlag(commonflags.GenerateKey, flags.Lookup(commonflags.GenerateKey))
			_ = viper.BindPFlag(commonflags.WalletPath, flags.Lookup(commonflags.WalletPath))
			_ = viper.BindPFlag(commonflags.Account, flags.Lookup(commonflags.Account))
		},
	}

	signCmd = &cobra.Command{
		Use:   "sign",
		Short: "Sign NeoFS structure",
	}

	signBearerCmd = &cobra.Command{
		Use:   "bearer-token",
		Short: "Sign bearer token to use it in requests",
		Run:   signBearerToken,
	}

	signSessionCmd = &cobra.Command{
		Use:   "session-token",
		Short: "Sign session token to use it in requests",
		Run:   signSessionToken,
	}

	convertCmd = &cobra.Command{
		Use:   "convert",
		Short: "Convert representation of NeoFS structures",
	}

	convertEACLCmd = &cobra.Command{
		Use:   "eacl",
		Short: "Convert representation of extended ACL table",
		Run:   convertEACLTable,
	}

	keyerCmd = &cobra.Command{
		Use:   "keyer",
		Short: "Generate or print information about keys",
		Run:   processKeyer,
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

func initUtilKeyerCmd() {
	keyerCmd.Flags().BoolP("generate", "g", false, "generate new private key")
	keyerCmd.Flags().Bool("hex", false, "print all values in hex encoding")
	keyerCmd.Flags().BoolP("uncompressed", "u", false, "use uncompressed public key format")
	keyerCmd.Flags().BoolP("multisig", "m", false, "calculate multisig address from public keys")
}

func initUtilSignBearerCmd() {
	commonflags.InitWithoutRPC(signBearerCmd)

	flags := signBearerCmd.Flags()

	flags.String("from", "", "File with JSON or binary encoded bearer token to sign")
	_ = signBearerCmd.MarkFlagFilename("from")
	_ = signBearerCmd.MarkFlagRequired("from")

	flags.String("to", "", "File to dump signed bearer token (default: binary encoded)")
	flags.Bool("json", false, "Dump bearer token in JSON encoding")
}

func initUtilSignSessionCmd() {
	commonflags.InitWithoutRPC(signSessionCmd)

	flags := signSessionCmd.Flags()

	flags.String("from", "", "File with JSON encoded session token to sign")
	_ = signSessionCmd.MarkFlagFilename("from")
	_ = signSessionCmd.MarkFlagRequired("from")

	flags.String("to", "", "File to save signed session token (optional)")
}

func initUtilConvertEACLCmd() {
	flags := convertEACLCmd.Flags()

	flags.String("from", "", "File with JSON or binary encoded extended ACL table")
	_ = convertEACLCmd.MarkFlagFilename("from")
	_ = convertEACLCmd.MarkFlagRequired("from")

	flags.String("to", "", "File to dump extended ACL table (default: binary encoded)")
	flags.Bool("json", false, "Dump extended ACL table in JSON encoding")
}

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

func initUtilLocodeInfoCmd() {
	flags := locodeInfoCmd.Flags()

	flags.StringVar(&locodeInfoDBPath, locodeInfoDBFlag, "", "Path to NeoFS UN/LOCODE database")
	_ = locodeInfoCmd.MarkFlagRequired(locodeInfoDBFlag)

	flags.StringVar(&locodeInfoCode, locodeInfoCodeFlag, "", "UN/LOCODE")
	_ = locodeInfoCmd.MarkFlagRequired(locodeInfoCodeFlag)
}

func init() {
	rootCmd.AddCommand(utilCmd)

	utilCmd.AddCommand(
		signCmd,
		convertCmd,
		keyerCmd,
		locodeCmd,
	)

	signCmd.AddCommand(signBearerCmd, signSessionCmd)
	convertCmd.AddCommand(convertEACLCmd)
	locodeCmd.AddCommand(locodeGenerateCmd, locodeInfoCmd)

	initUtilKeyerCmd()

	initUtilSignBearerCmd()
	initUtilSignSessionCmd()

	initUtilConvertEACLCmd()

	initUtilLocodeInfoCmd()
	initUtilLocodeGenerateCmd()
}

func signBearerToken(cmd *cobra.Command, _ []string) {
	btok, err := getBearerToken(cmd, "from")
	common.ExitOnErr(cmd, "", err)

	pk := key.GetOrGenerate(cmd)

	err = btok.Sign(*pk)
	common.ExitOnErr(cmd, "", err)

	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool("json")

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = btok.MarshalJSON()
		common.ExitOnErr(cmd, "can't JSON encode bearer token: %w", err)
	} else {
		data = btok.Marshal()
	}

	if len(to) == 0 {
		prettyPrintJSON(cmd, data)

		return
	}

	err = os.WriteFile(to, data, 0644)
	common.ExitOnErr(cmd, "can't write signed bearer token to file: %w", err)

	cmd.Printf("signed bearer token was successfully dumped to %s\n", to)
}

func signSessionToken(cmd *cobra.Command, _ []string) {
	path, err := cmd.Flags().GetString("from")
	common.ExitOnErr(cmd, "", err)

	stok, err := getSessionToken(path)
	if err != nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("can't read session token from %s: %w", path, err))
	}

	pk := key.GetOrGenerate(cmd)

	err = stok.Sign(pk)
	common.ExitOnErr(cmd, "can't sign token: %w", err)

	data, err := stok.MarshalJSON()
	common.ExitOnErr(cmd, "can't encode session token: %w", err)

	to := cmd.Flag("to").Value.String()
	if len(to) == 0 {
		prettyPrintJSON(cmd, data)
		return
	}

	err = os.WriteFile(to, data, 0644)
	if err != nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("can't write signed session token to %s: %w", to, err))
	}

	cmd.Printf("signed session token saved in %s\n", to)
}

func convertEACLTable(cmd *cobra.Command, _ []string) {
	pathFrom := cmd.Flag("from").Value.String()
	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool("json")

	table, err := parseEACL(pathFrom)
	common.ExitOnErr(cmd, "", err)

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = table.MarshalJSON()
		common.ExitOnErr(cmd, "can't JSON encode extended ACL table: %w", err)
	} else {
		data, err = table.Marshal()
		common.ExitOnErr(cmd, "can't binary encode extended ACL table: %w", err)
	}

	if len(to) == 0 {
		prettyPrintJSON(cmd, data)
		return
	}
	err = os.WriteFile(to, data, 0644)
	common.ExitOnErr(cmd, "can't write exteded ACL table to file: %w", err)

	cmd.Printf("extended ACL table was successfully dumped to %s\n", to)
}

func processKeyer(cmd *cobra.Command, args []string) {
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
			common.ExitOnErr(cmd, "", errKeyerSingleArgument)
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

	common.ExitOnErr(cmd, "", err)

	result.PrettyPrint(uncompressed, useHex)
}

func prettyPrintJSON(cmd *cobra.Command, data []byte) {
	buf := new(bytes.Buffer)
	if err := json.Indent(buf, data, "", "  "); err != nil {
		common.PrintVerbose("Can't pretty print json: %w", err)
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
		return os.WriteFile(filename, key, 0600)
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
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't open %v file: %w", filename, err)
	}

	return d.ParseBinary(data)
}
