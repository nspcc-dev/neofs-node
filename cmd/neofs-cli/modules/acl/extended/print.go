package extended

import (
	"fmt"
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/spf13/cobra"
)

var printEACLCmd = &cobra.Command{
	Use:   "print",
	Short: "Pretty print extended ACL from the file(in text or json format) or for given container.",
	Args:  cobra.NoArgs,
	RunE:  printEACL,
}

func init() {
	flags := printEACLCmd.Flags()
	flags.StringP("file", "f", "",
		"Read list of extended ACL table records from text or json file")
	_ = printEACLCmd.MarkFlagRequired("file")
}

func printEACL(cmd *cobra.Command, _ []string) error {
	file, _ := cmd.Flags().GetString("file")
	eaclTable := new(eacl.Table)
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("can't read file with EACL: %w", err)
	}

	if strings.HasSuffix(file, ".json") {
		if err := eaclTable.UnmarshalJSON(data); err != nil {
			return fmt.Errorf("unable to parse json: %w", err)
		}
	} else {
		rules := strings.Split(strings.TrimSpace(string(data)), "\n")
		if err := util.ParseEACLRules(eaclTable, rules); err != nil {
			return fmt.Errorf("can't parse file with EACL: %w", err)
		}
	}
	util.PrettyPrintTableEACL(cmd, eaclTable)
	return nil
}
