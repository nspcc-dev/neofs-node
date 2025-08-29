package fschain

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var estimationsCmd = &cobra.Command{
	Use:   "estimations",
	Short: "See container estimations based on reports made by storage nodes",
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: estimationsFunc,
	Args: cobra.NoArgs,
}

func estimationsFunc(cmd *cobra.Command, _ []string) error {
	var cidRequested cid.ID
	cIDString, err := cmd.Flags().GetString(estimationsContainerFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", estimationsContainerFlag, err))
	}
	if cIDString != "" {
		if err := cidRequested.DecodeString(cIDString); err != nil {
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

	epoch, err := cmd.Flags().GetInt64(estimationsEpochFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", estimationsEpochFlag, err))
	}

	if epoch <= 0 {
		netmapHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
		if err != nil {
			return fmt.Errorf("netmap contract hash resolution: %w", err)
		}

		epochFromContract, err := unwrap.Int64(inv.Call(netmapHash, "epoch"))
		if err != nil {
			return fmt.Errorf("reading epoch: %w", err)
		}

		epoch = epochFromContract + epoch // epoch is negative here
	}

	cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
	if err != nil {
		return fmt.Errorf("container contract hash resolution: %w", err)
	}

	sID, iter, err := unwrap.SessionIterator(inv.Call(cnrHash, "iterateAllEstimations", epoch))
	if err != nil {
		return fmt.Errorf("iterator expansion: %w", err)
	}

	defer func() {
		if (sID != uuid.UUID{}) {
			_ = inv.TerminateSession(sID)
		}
	}()

	ee, err := parseEstimations(cmd, inv, sID, iter)
	if err != nil {
		return fmt.Errorf("parsing estimations read from contract: %w", err)
	}

	if len(ee) == 0 {
		cmd.Printf("No estimations found in %d epoch\n", epoch)
		return nil
	}

	printEstimations(cmd, epoch, cidRequested, ee)

	return nil
}

func parseEstimations(cmd *cobra.Command, inv *invoker.Invoker, sID uuid.UUID, iter result.Iterator) (map[cid.ID]container.EstimationV2, error) {
	items := make([]stackitem.Item, 0)

	for {
		ii, err := inv.TraverseIterator(sID, &iter, config.DefaultMaxIteratorResultItems)
		if err != nil {
			return nil, fmt.Errorf("iterator traversal; session: %s, error: %w", sID, err)
		}

		if len(ii) == 0 {
			break
		}

		items = append(items, ii...)
	}

	ee := make(map[cid.ID]container.EstimationV2, len(items))
	for i := range items {
		kv, err := client.ArrayFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("%d item on stack is not an array: %w", i, err)
		}
		if len(kv) != 2 {
			return nil, fmt.Errorf("%d item on stack is not a key-value pair", i)
		}
		cidRaw, err := client.BytesFromStackItem(kv[0])
		if err != nil {
			return nil, fmt.Errorf("%d item's key reading: %w", i, err)
		}
		var cID cid.ID
		err = cID.Decode(cidRaw)
		if err != nil {
			return nil, fmt.Errorf("%d item's key reading as a container ID: %w", i, err)
		}
		var est container.EstimationV2
		err = est.FromStackItem(kv[1])
		if err != nil {
			return nil, fmt.Errorf("%d item's value reading as a container estimation: %w", i, err)
		}

		if old, found := ee[cID]; found {
			cmd.Printf("WARN: found duplicated estimations for %s container "+
				"(sizes: %d and %d; number of objects: %d and %d)\n",
				cID,
				old.StorageSize, est.StorageSize,
				old.NumberOfObjects, est.NumberOfObjects,
			)

			continue
		}
		ee[cID] = est
	}

	return ee, nil
}

func printEstimations(cmd *cobra.Command, epoch int64, cID cid.ID, ee map[cid.ID]container.EstimationV2) {
	printer := func(cmd *cobra.Command, cID cid.ID, e container.EstimationV2) {
		cmd.Printf("%s: container size: %d; number of objects: %d\n", cID, e.StorageSize, e.NumberOfObjects)
	}

	if !cID.IsZero() {
		if e, ok := ee[cID]; ok {
			printer(cmd, cID, e)
			return
		}
		cmd.Printf("No estimations found for %s container in %d epoch\n", cID, epoch)

		return
	}

	cmd.Printf("Estimations for %d epoch:\n", epoch)

	sortedCIDs := slices.SortedFunc(maps.Keys(ee), func(cID1 cid.ID, cID2 cid.ID) int {
		return bytes.Compare(cID1[:], cID2[:])
	})
	for _, cID := range sortedCIDs {
		printer(cmd, cID, ee[cID])
	}
}
