package container

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

// flags of list-object command.
const (
	flagListObjectPrintAttr = "with-attr"
)

// flag vars of list-objects command.
var (
	flagVarListObjectsPrintAttr bool
)

var listContainerObjectsCmd = &cobra.Command{
	Use:   "list-objects",
	Short: "List existing objects in container",
	Long:  `List existing objects in container`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		id, err := parseContainerID()
		if err != nil {
			return err
		}

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		pk, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}
		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}

		var prmSearch internalclient.SearchObjectsPrm
		var prmHead internalclient.HeadObjectPrm

		prmSearch.SetClient(cli)

		if flagVarListObjectsPrintAttr {
			prmHead.SetClient(cli)
			prmHead.SetPrivateKey(*pk)
			err = objectCli.Prepare(cmd, &prmSearch, &prmHead)
			if err != nil {
				return err
			}
		} else {
			err = objectCli.Prepare(cmd, &prmSearch)
			if err != nil {
				return err
			}
		}

		prmSearch.SetPrivateKey(*pk)
		prmSearch.SetContainerID(id)
		prmSearch.SetFilters(*filters)

		res, err := internalclient.SearchObjects(ctx, prmSearch)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		objectIDs := res.IDList()

		for i := range objectIDs {
			cmd.Println(objectIDs[i].String())

			if flagVarListObjectsPrintAttr {
				var addr oid.Address
				addr.SetContainer(id)
				addr.SetObject(objectIDs[i])
				prmHead.SetAddress(addr)

				resHead, err := internalclient.HeadObject(ctx, prmHead)
				if err == nil {
					attrs := resHead.Header().UserAttributes()
					for i := range attrs {
						key := attrs[i].Key()
						val := attrs[i].Value()

						if key == object.AttributeTimestamp {
							cmd.Printf("  %s: %s (%s)\n", key, val, common.PrettyPrintUnixTime(val))
							continue
						}

						cmd.Printf("  %s: %s\n", key, val)
					}
				} else {
					cmd.Printf("  failed to read attributes: %v\n", err)
				}
			}
		}
		return nil
	},
}

func initContainerListObjectsCmd() {
	commonflags.Init(listContainerObjectsCmd)
	objectCli.InitBearer(listContainerObjectsCmd)

	flags := listContainerObjectsCmd.Flags()

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.BoolVar(&flagVarListObjectsPrintAttr, flagListObjectPrintAttr, false,
		"Request and print user attributes of each object",
	)
}
