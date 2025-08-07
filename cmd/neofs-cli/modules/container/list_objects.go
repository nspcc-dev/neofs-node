package container

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
		bt, err := common.ReadBearerToken(cmd, objectCli.BearerTokenFlag)
		if err != nil {
			return err
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		var prmHead client.PrmObjectHead
		if flagVarListObjectsPrintAttr {
			err = objectCli.Prepare(cmd, &prmHead)
			if err != nil {
				return err
			}
		}

		var opts client.SearchObjectsOptions
		ttl := viper.GetUint32(commonflags.TTL)
		common.PrintVerbose(cmd, "TTL: %d", ttl)
		if ttl == 1 {
			opts.DisableForwarding()
		}
		if bt != nil {
			opts.WithBearerToken(*bt)
		}
		opts.WithXHeaders(objectCli.ParseXHeaders(cmd)...)

		var res []client.SearchResultItem
		var cursor string
		for {
			res, cursor, err = cli.SearchObjects(ctx, id, *filters, nil, cursor, (*neofsecdsa.Signer)(pk), opts)
			if err != nil {
				return fmt.Errorf("rpc error: read object header via client: %w", err)
			}
			for i := range res {
				cmd.Println(res[i].ID)
				if !flagVarListObjectsPrintAttr {
					continue
				}

				hdr, err := cli.ObjectHead(ctx, id, res[i].ID, user.NewAutoIDSigner(*pk), prmHead)
				if err == nil {
					attrs := hdr.UserAttributes()
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
			if cursor == "" {
				break
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
