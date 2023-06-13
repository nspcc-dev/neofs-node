package container

import (
	"strings"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
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
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		id := parseContainerID(cmd)

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		cli := internalclient.GetSDKClientByFlag(ctx, cmd, key.GetOrGenerate(cmd), commonflags.RPC)

		var prmSearch internalclient.SearchObjectsPrm
		var prmHead internalclient.HeadObjectPrm

		prmSearch.SetClient(cli)

		if flagVarListObjectsPrintAttr {
			prmHead.SetClient(cli)
			objectCli.Prepare(cmd, &prmSearch, &prmHead)
		} else {
			objectCli.Prepare(cmd, &prmSearch)
		}

		prmSearch.SetContainerID(id)
		prmSearch.SetFilters(*filters)

		res, err := internalclient.SearchObjects(ctx, prmSearch)
		common.ExitOnErr(cmd, "rpc error: %w", err)

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
					attrs := resHead.Header().Attributes()
					for i := range attrs {
						attrKey := attrs[i].Key()
						if !strings.HasPrefix(attrKey, v2object.SysAttributePrefix) {
							// FIXME(@cthulhu-rider): neofs-sdk-go#226 use dedicated method to skip system attributes
							cmd.Printf("  %s: %s\n", attrKey, attrs[i].Value())
						}
					}
				} else {
					cmd.Printf("  failed to read attributes: %v\n", err)
				}
			}
		}
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
