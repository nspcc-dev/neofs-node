package util

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/keyer"
	"github.com/spf13/cobra"
)

var keyerCmd = &cobra.Command{
	Use:   "keyer",
	Short: "Generate or print information about keys",
	Run:   processKeyer,
}

var errKeyerSingleArgument = errors.New("pass only one argument at a time")

func initKeyerCmd() {
	keyerCmd.Flags().BoolP("generate", "g", false, "generate new private key")
	keyerCmd.Flags().Bool("hex", false, "print all values in hex encoding")
	keyerCmd.Flags().BoolP("uncompressed", "u", false, "use uncompressed public key format")
	keyerCmd.Flags().BoolP("multisig", "m", false, "calculate multisig address from public keys")
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
	return !os.IsNotExist(err) && !info.IsDir()
}

func keyerParseFile(filename string, d *keyer.Dashboard) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't open %v file: %w", filename, err)
	}

	return d.ParseBinary(data)
}
