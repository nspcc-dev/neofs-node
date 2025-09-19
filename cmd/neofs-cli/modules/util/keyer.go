package util

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/util/keyer"
	"github.com/spf13/cobra"
)

var keyerCmd = &cobra.Command{
	Use:   "keyer",
	Short: "Generate or print information about keys",
	RunE:  processKeyer,
}

var errKeyerSingleArgument = errors.New("pass only one argument at a time")

func initKeyerCmd() {
	keyerCmd.Flags().BoolP("generate", "g", false, "Generate new private key")
	keyerCmd.Flags().Bool("hex", false, "Print all values in hex encoding")
	keyerCmd.Flags().BoolP("uncompressed", "u", false, "Use uncompressed public key format")
	keyerCmd.Flags().BoolP("multisig", "m", false, "Calculate multisig address from public keys")
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

func keyerGenerate(filename string, d *keyer.Dashboard) error {
	key := make([]byte, keyer.NeoPrivateKeySize)

	_, _ = rand.Read(key)

	err := d.ParseBinary(key)
	if err != nil {
		return fmt.Errorf("can't parse key: %w", err)
	}

	if filename != "" {
		return os.WriteFile(filename, key, 0o600)
	}

	return nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return !errors.Is(err, fs.ErrNotExist) && !info.IsDir()
}

func keyerParseFile(filename string, d *keyer.Dashboard) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("can't open %v file: %w", filename, err)
	}

	return d.ParseBinary(data)
}
