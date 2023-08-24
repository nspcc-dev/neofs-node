package extended

import (
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/spf13/cobra"
)

var printEACLCmd = &cobra.Command{
	Use:   "print",
	Short: "Pretty print extended ACL from the file(in text or json format) or for given container.",
	Args:  cobra.NoArgs,
	Run:   printEACL,
}

func init() {
	flags := printEACLCmd.Flags()
	flags.StringP("file", "f", "",
		"Read list of extended ACL table records from text or json file")
	_ = printEACLCmd.MarkFlagRequired("file")
}

func printEACL(cmd *cobra.Command, _ []string) {
	file, _ := cmd.Flags().GetString("file")
	eaclTable := new(eacl.Table)
	data, err := os.ReadFile(file)
	common.ExitOnErr(cmd, "can't read file with EACL: %w", err)
	if strings.HasSuffix(file, ".json") {
		common.ExitOnErr(cmd, "unable to parse json: %w", eaclTable.UnmarshalJSON(data))
	} else {
		rules := strings.Split(strings.TrimSpace(string(data)), "\n")
		common.ExitOnErr(cmd, "can't parse file with EACL: %w", util.ParseEACLRules(eaclTable, rules))
	}
	util.PrettyPrintTableEACL(cmd, eaclTable)
}
