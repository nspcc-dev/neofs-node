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

func init() {
	rootCmd.AddCommand(utilCmd)

	utilCmd.AddCommand(signCmd)
	utilCmd.AddCommand(convertCmd)
	utilCmd.AddCommand(keyerCmd)

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
		data, err = btok.ToV2().StableMarshal(nil)
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
		data, err = table.ToV2().StableMarshal(nil)
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

		// back to SDK token
		btok = token.NewBearerTokenFromV2(v2)
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
