package container

import (
	"context"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

const (
	fromFlag      = "from"
	fromFlagUsage = "Path to file with encoded container"
)

var (
	containerID       string
	containerPathFrom string
	containerPathTo   string
	containerJSON     bool
)

var getContainerInfoCmd = &cobra.Command{
	Use:   "get",
	Short: "Get container field info",
	Long:  `Get container field info`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cnr := getContainer(ctx, cmd)

		prettyPrintContainer(cmd, cnr, containerJSON)

		if containerPathTo != "" {
			var (
				data []byte
				err  error
			)

			if containerJSON {
				data, err = cnr.MarshalJSON()
				common.ExitOnErr(cmd, "can't JSON encode container: %w", err)
			} else {
				data = cnr.Marshal()
			}

			err = os.WriteFile(containerPathTo, data, 0644)
			common.ExitOnErr(cmd, "can't write container to file: %w", err)
		}
	},
}

func initContainerInfoCmd() {
	commonflags.Init(getContainerInfoCmd)

	flags := getContainerInfoCmd.Flags()

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathTo, "to", "", "Path to dump encoded container")
	flags.StringVar(&containerPathFrom, fromFlag, "", fromFlagUsage)
	flags.BoolVar(&containerJSON, commonflags.JSON, false, "Print or dump container in JSON format")
}

type stringWriter cobra.Command

func (x *stringWriter) WriteString(s string) (n int, err error) {
	(*cobra.Command)(x).Print(s)
	return len(s), nil
}

func prettyPrintContainer(cmd *cobra.Command, cnr container.Container, jsonEncoding bool) {
	if jsonEncoding {
		common.PrettyPrintJSON(cmd, cnr, "container")
		return
	}

	var id cid.ID
	cnr.CalculateID(&id)
	cmd.Println("container ID:", id)

	cmd.Println("owner ID:", cnr.Owner())

	basicACL := cnr.BasicACL()
	prettyPrintBasicACL(cmd, basicACL)

	cmd.Println("created:", cnr.CreatedAt())

	cmd.Println("attributes:")
	cnr.IterateAttributes(func(key, val string) {
		cmd.Printf("\t%s=%s\n", key, val)
	})

	cmd.Println("placement policy:")
	common.ExitOnErr(cmd, "write policy: %w", cnr.PlacementPolicy().WriteStringTo((*stringWriter)(cmd)))
	cmd.Println()
}

func prettyPrintBasicACL(cmd *cobra.Command, basicACL acl.Basic) {
	cmd.Printf("basic ACL: %s", basicACL.EncodeToString())

	var prettyName string

	switch basicACL {
	case acl.Private:
		prettyName = acl.NamePrivate
	case acl.PrivateExtended:
		prettyName = acl.NamePrivateExtended
	case acl.PublicRO:
		prettyName = acl.NamePublicRO
	case acl.PublicROExtended:
		prettyName = acl.NamePublicROExtended
	case acl.PublicRW:
		prettyName = acl.NamePublicRW
	case acl.PublicRWExtended:
		prettyName = acl.NamePublicRWExtended
	case acl.PublicAppend:
		prettyName = acl.NamePublicAppend
	case acl.PublicAppendExtended:
		prettyName = acl.NamePublicAppendExtended
	}

	if prettyName != "" {
		cmd.Printf(" (%s)", prettyName)
	}

	cmd.Println()
	util.PrettyPrintTableBACL(cmd, &basicACL)
}

func getContainer(ctx context.Context, cmd *cobra.Command) container.Container {
	var cnr container.Container
	if containerPathFrom != "" {
		data, err := os.ReadFile(containerPathFrom)
		common.ExitOnErr(cmd, "can't read file: %w", err)

		err = cnr.Unmarshal(data)
		common.ExitOnErr(cmd, "can't unmarshal container: %w", err)
	} else {
		id := parseContainerID(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		var prm internalclient.GetContainerPrm
		prm.SetClient(cli)
		prm.SetContainer(id)

		res, err := internalclient.GetContainer(ctx, prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		cnr = res.Container()
	}
	return cnr
}
