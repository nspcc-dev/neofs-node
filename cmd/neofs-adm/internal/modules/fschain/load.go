package fschain

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cnrrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	reportsCmd = &cobra.Command{
		Use:   "load-report",
		Short: "Inspect storage load reports",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: reportsFunc,
		Args: cobra.NoArgs,
	}
)

func reportsFunc(cmd *cobra.Command, _ []string) error {
	cIDString, err := cmd.Flags().GetString(containerIDFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", containerIDFlag, err))
	}
	var cID cid.ID
	err = cID.DecodeString(cIDString)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)
	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}
	cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
	if err != nil {
		return fmt.Errorf("container contract hash resolution: %w", err)
	}
	cnrReader := cnrrpc.NewReader(inv, cnrHash)

	sID, iter, err := cnrReader.IterateReports(util.Uint256(cID))
	if err != nil {
		return fmt.Errorf("iterator expansion: %w", err)
	}
	defer func() {
		if sID != uuid.Nil {
			_ = inv.TerminateSession(sID)
		}
	}()

	items := make([]stackitem.Item, 0)
	for {
		ii, err := inv.TraverseIterator(sID, &iter, config.DefaultMaxIteratorResultItems)
		if err != nil {
			return fmt.Errorf("iterator traversal; session: %s, error: %w", sID, err)
		}

		if len(ii) == 0 {
			break
		}

		items = append(items, ii...)
	}
	if len(items) == 0 {
		cmd.Println("No reports found")
		return nil
	}

	var r cnrrpc.ContainerNodeReport
	for i, item := range items {
		err := r.FromStackItem(item)
		if err != nil {
			return fmt.Errorf("reading %d stackitem as a raw node report: %w", i, err)
		}

		cmd.Printf("Report #%d:\n  Reporter's pubic Key: %s:\n", i, r.PublicKey)
		cmd.Printf("  Size: %d\n", r.ContainerSize)
		cmd.Printf("  Objects: %d\n", r.NumberOfObjects)
		cmd.Printf("  Update epoch: %d\n", r.LastUpdateEpoch)
		cmd.Printf("  Reports number: %d\n", r.NumberOfReports)
		if i != len(items)-1 {
			cmd.Println()
		}
	}

	return nil
}

var loadSummaryCmd = &cobra.Command{
	Use:     "load-summary",
	Example: "load-summary -- [<cid>]",
	Short:   "Get container load summary",
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: loadSummaryFunc,
	Args: cobra.RangeArgs(0, 1),
}

func loadSummaryFunc(cmd *cobra.Command, args []string) error {
	var cID cid.ID
	if len(args) > 0 {
		err := cID.DecodeString(args[0])
		if err != nil {
			return fmt.Errorf("invalid container ID: %w", err)
		}
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)
	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}
	cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
	if err != nil {
		return fmt.Errorf("container contract hash resolution: %w", err)
	}
	cnrReader := cnrrpc.NewReader(inv, cnrHash)

	if cID.IsZero() {
		sID, iter, err := cnrReader.IterateAllReportSummaries()
		if err != nil {
			return fmt.Errorf("failed to open session: %w", err)
		}
		defer func() {
			if sID != uuid.Nil {
				_ = inv.TerminateSession(sID)
			}
		}()

		for {
			ii, err := inv.TraverseIterator(sID, &iter, config.DefaultMaxIteratorResultItems)
			if err != nil {
				return fmt.Errorf("iterator traversal; session: %s, error: %w", sID, err)
			}

			if len(ii) == 0 {
				break
			}

			for _, i := range ii {
				kv := i.Value().([]stackitem.Item)

				cidRaw, err := kv[0].TryBytes()
				if err != nil {
					return fmt.Errorf("summary's container ID is not bytes: %w", err)
				}
				cID, err = cid.DecodeBytes(cidRaw)
				if err != nil {
					return fmt.Errorf("invalid container ID: %w", err)
				}

				var sum cnrrpc.ContainerNodeReportSummary
				err = sum.FromStackItem(kv[1])
				if err != nil {
					return fmt.Errorf("decoding summary: %w", err)
				}

				cmd.Printf("%s: Number of objects: %d, container size: %d\n", cID, sum.NumberOfObjects, sum.ContainerSize)
			}
		}
	} else {
		sum, err := cnrReader.GetNodeReportSummary(util.Uint256(cID))
		if err != nil {
			return fmt.Errorf("rpc call: %w", err)
		}
		cmd.Printf("Number of objects: %d, container size: %d\n", sum.NumberOfObjects, sum.ContainerSize)
	}

	return nil
}
