package util

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

var convertEACLCmd = &cobra.Command{
	Use:   "eacl",
	Short: "Convert representation of extended ACL table",
	Args:  cobra.NoArgs,
	RunE:  convertEACLTable,
}

func initConvertEACLCmd() {
	flags := convertEACLCmd.Flags()

	flags.String("from", "", "File with JSON or binary encoded extended ACL table")
	_ = convertEACLCmd.MarkFlagFilename("from")
	_ = convertEACLCmd.MarkFlagRequired("from")

	flags.String("to", "", "File to dump extended ACL table (default: binary encoded)")
	flags.Bool(commonflags.JSON, false, "Dump extended ACL table in JSON encoding")
}

func convertEACLTable(cmd *cobra.Command, _ []string) error {
	pathFrom := cmd.Flag("from").Value.String()
	to := cmd.Flag("to").Value.String()
	jsonFlag, _ := cmd.Flags().GetBool(commonflags.JSON)

	table, err := common.ReadEACL(cmd, pathFrom)
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
		data = table.Marshal()
	}

	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, table, "eACL")
		return nil
	}

	err = os.WriteFile(to, data, 0o644)
	if err != nil {
		return fmt.Errorf("can't write exteded ACL table to file: %w", err)
	}

	cmd.Printf("extended ACL table was successfully dumped to %s\n", to)

	return nil
}
