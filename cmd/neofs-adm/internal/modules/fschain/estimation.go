package fschain

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var estimationsCmd = &cobra.Command{
	Use:   "estimations",
	Short: "See container estimations reported by storage nodes",
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: estimationsFunc,
	Args: cobra.NoArgs,
}

func estimationsFunc(cmd *cobra.Command, _ []string) error {
	cIDString, err := cmd.Flags().GetString(estimationsContainerFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", estimationsContainerFlag, err))
	}

	var cID cid.ID
	if err := cID.DecodeString(cIDString); err != nil {
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

	sID, iter, err := unwrap.SessionIterator(inv.Call(cnrHash, "iterateContainerSizes", epoch, cID[:]))
	if err != nil {
		return fmt.Errorf("iterator expansion: %w", err)
	}

	defer func() {
		if (sID != uuid.UUID{}) {
			_ = inv.TerminateSession(sID)
		}
	}()

	ee, err := parseEstimations(inv, sID, iter)
	if err != nil {
		return fmt.Errorf("parsing estimations read from contract: %w", err)
	}

	if len(ee) == 0 {
		cmd.Printf("No estimations found for %s container in %d epoch\n", cID, epoch)
		return nil
	}

	printEstimations(cmd, epoch, ee)

	return nil
}

func parseEstimations(inv *invoker.Invoker, sID uuid.UUID, iter result.Iterator) ([]container.Estimation, error) {
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

	ee := make([]container.Estimation, len(items))
	for i := range items {
		err := ee[i].FromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("parsing stack item: %w", err)
		}
	}

	return ee, nil
}

func printEstimations(cmd *cobra.Command, epoch int64, ee []container.Estimation) {
	cmd.Printf("Estimations for %d epoch:\n", epoch)

	for _, estimation := range ee {
		reporterString := base58.Encode(estimation.Reporter)
		cmd.Printf("Container size: %d, reporter's key (base58 encoding): %s\n", estimation.Size, reporterString)
	}
}
