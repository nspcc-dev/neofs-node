package util

import (
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

var convertEACLCmd = &cobra.Command{
	Use:   "eacl",
	Short: "Convert representation of extended ACL table",
	Args:  cobra.NoArgs,
	Run:   convertEACLTable,
}

func initConvertEACLCmd() {
	flags := convertEACLCmd.Flags()

	flags.String("from", "", "File with JSON or binary encoded extended ACL table")
	_ = convertEACLCmd.MarkFlagFilename("from")
	_ = convertEACLCmd.MarkFlagRequired("from")

	flags.String("to", "", "File to dump extended ACL table (default: binary encoded)")
	flags.Bool(commonflags.JSON, false, "Dump extended ACL table in JSON encoding")
}

func convertEACLTable(cmd *cobra.Command, _ []string) {
	pathFrom := cmd.Flag("from").Value.String()
	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool(commonflags.JSON)

	table := common.ReadEACL(cmd, pathFrom)

	var data []byte
	var err error
	if jsonFlag || len(to) == 0 {
		data, err = table.MarshalJSON()
		common.ExitOnErr(cmd, "can't JSON encode extended ACL table: %w", err)
	} else {
		data, err = table.Marshal()
		common.ExitOnErr(cmd, "can't binary encode extended ACL table: %w", err)
	}

	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, table, "eACL")
		return
	}

	err = os.WriteFile(to, data, 0o644)
	common.ExitOnErr(cmd, "can't write exteded ACL table to file: %w", err)

	cmd.Printf("extended ACL table was successfully dumped to %s\n", to)
}
