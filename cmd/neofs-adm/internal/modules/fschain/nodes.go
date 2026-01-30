package fschain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cnrrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/n3util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "See container list according to Container contract maintained by Alphabet nodes",
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: nodesFunc,
	Args: cobra.NoArgs,
}

func nodesFunc(cmd *cobra.Command, _ []string) error {
	cIDString, err := cmd.Flags().GetString(containerIDFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", containerIDFlag, err))
	}
	var cID cid.ID
	err = cID.DecodeString(cIDString)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	c, err := n3util.GetN3Client(viper.GetViper())
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

	for i := range math.MaxUint8 {
		err = readAndPrintContainerPlacementVector(cmd, inv, cnrReader, cID, i)
		if err != nil {
			if errors.Is(err, errNoMoreVectors) {
				if i == 0 {
					cmd.Println("Empty container nodes list.")
				}
				return nil
			}

			return err
		}
	}

	return nil
}

var errNoMoreVectors = errors.New("no more vectors found")

func readAndPrintContainerPlacementVector(cmd *cobra.Command, inv *invoker.Invoker, cnrReader *cnrrpc.ContractReader, cID cid.ID, index int) error {
	sID, iter, err := cnrReader.Nodes(util.Uint256(cID[:]), big.NewInt(int64(index)))
	if err != nil {
		return fmt.Errorf("iterator expansion: %w", err)
	}
	defer func() {
		if (sID != uuid.UUID{}) {
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
		return errNoMoreVectors
	}

	cmd.Printf("%d vector:\n", index)
	for i, item := range items {
		pk, err := item.TryBytes()
		if err != nil {
			return fmt.Errorf("reading %d stackitem of %d vector as raw public key: %w", i, index, err)
		}

		cmd.Println("\t" + hex.EncodeToString(pk))
	}

	return nil
}
