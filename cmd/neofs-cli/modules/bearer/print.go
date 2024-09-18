package bearer

import (
	"fmt"
	"io"
	"os"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/spf13/cobra"
)

var printCmd = &cobra.Command{
	Use:   "print",
	Short: "Print binary-marshalled bearer tokens from file or STDIN in JSON format",
	Long: `neofs-cli bearer print [FILE]
With no FILE, or when FILE is -, read standard input.`,
	Args: cobra.MaximumNArgs(1),
	RunE: printToken,
}

func printToken(cmd *cobra.Command, arg []string) error {
	var reader io.Reader

	if len(arg) == 1 && arg[0] != "-" {
		var err error

		reader, err = os.Open(arg[0])
		if err != nil {
			return fmt.Errorf("opening file: %w", err)
		}
	} else {
		reader = cmd.InOrStdin()
	}

	raw, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("reading input data failed: %w", err)
	}

	var token bearer.Token
	err = token.Unmarshal(raw)
	if err != nil {
		return fmt.Errorf("invalid binary token: %w", err)
	}

	rawJSON, err := token.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling read token in JSON format: %w", err)
	}

	cmd.Print(string(rawJSON))
	return nil
}
