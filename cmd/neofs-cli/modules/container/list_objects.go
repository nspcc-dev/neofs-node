package container

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var listContainerObjectsCmd = &cobra.Command{
	Use:   "list-objects",
	Short: "List existing objects in container",
	Long:  `List existing objects in container`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		pk := key.GetOrGenerate(cmd)

		var prm internalclient.SearchObjectsPrm
		sessionCli.Prepare(cmd, id, nil, pk, &prm)
		objectCli.Prepare(cmd, &prm)
		prm.SetContainerID(id)
		prm.SetFilters(*filters)

		res, err := internalclient.SearchObjects(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		objectIDs := res.IDList()

		for i := range objectIDs {
			cmd.Println(objectIDs[i].String())
		}
	},
}

func initContainerListObjectsCmd() {
	commonflags.Init(listContainerObjectsCmd)
	objectCli.InitBearer(listContainerObjectsCmd)

	flags := listContainerObjectsCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
}
