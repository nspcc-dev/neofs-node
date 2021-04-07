package modules

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-paymentd/modules/db"
	"github.com/spf13/cobra"
)

var (
	historyCmd = &cobra.Command{
		Use:   "history",
		Short: "Detailed history of all asset transfers",
		Long:  "Detailed history of all asset transfers",
		RunE:  history,
	}
)

func init() {
	rootCmd.AddCommand(historyCmd)
	historyCmd.Flags().String("neo-endpoint", "", "N3 side chain RPC node endpoint")
	historyCmd.Flags().Uint32P("start-from", "s", 0, "Starting block for history indexing")
	historyCmd.Flags().StringP("address", "a", "", "NeoFS internal balance address")
	historyCmd.Flags().String("db", "", "Database of already indexed values") // todo
}

func history(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	endpoint, _ := cmd.Flags().GetString("neo-endpoint")
	index, _ := cmd.Flags().GetUint32("start-from")
	addr, _ := cmd.Flags().GetString("address")

	target, err := address.StringToUint160(addr)
	if err != nil {
		return fmt.Errorf("can't parse address: %w", err)
	}

	cli, err := client.New(ctx, endpoint, client.Options{})
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	if err = cli.Init(); err != nil {
		return fmt.Errorf("can't initialize RPC connection with %s: %d", endpoint, err)
	}

	historyDB, err := db.BuildDB(cli, index, target)
	if err != nil {
		return fmt.Errorf("error at building history: %w", err)
	}

	fmt.Print(historyDB)

	return nil
}
