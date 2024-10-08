package basic

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	"github.com/spf13/cobra"
)

var printACLCmd = &cobra.Command{
	Use:     "print",
	Short:   "Pretty print basic ACL from the HEX representation",
	Example: `neofs-cli acl basic print 0x1C8C8CCC`,
	Long: `Pretty print basic ACL from the HEX representation or keyword.
Few roles have exclusive default access to set of operation, even if particular bit deny it.
Container have access to the operations of the data replication mechanism:
    Get, Head, Put, Search, Hash.
InnerRing members are allowed to data audit ops only:
    Get, Head, Hash, Search.`,
	RunE: printACL,
	Args: cobra.ExactArgs(1),
}

func printACL(cmd *cobra.Command, args []string) error {
	var bacl acl.Basic
	if err := bacl.DecodeString(args[0]); err != nil {
		return fmt.Errorf("unable to parse basic acl: %w", err)
	}

	util.PrettyPrintTableBACL(cmd, &bacl)
	return nil
}
