package container

import (
	"context"
	"fmt"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cnr, err := getContainer(ctx, cmd)
		if err != nil {
			return err
		}

		err = prettyPrintContainer(cmd, cnr, containerJSON)
		if err != nil {
			return err
		}

		if containerPathTo != "" {
			var (
				data []byte
				err  error
			)

			if containerJSON {
				data, err = cnr.MarshalJSON()
				if err != nil {
					return fmt.Errorf("can't JSON encode container: %w", err)
				}
			} else {
				data = cnr.Marshal()
			}

			err = os.WriteFile(containerPathTo, data, 0644)
			if err != nil {
				return fmt.Errorf("can't write container to file: %w", err)
			}
		}
		return nil
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

func prettyPrintContainer(cmd *cobra.Command, cnr container.Container, jsonEncoding bool) error {
	if jsonEncoding {
		common.PrettyPrintJSON(cmd, cnr, "container")
		return nil
	}

	id := cid.NewFromMarshalledContainer(cnr.Marshal())
	cmd.Println("container ID:", id)

	cmd.Println("owner ID:", cnr.Owner())

	basicACL := cnr.BasicACL()
	prettyPrintBasicACL(cmd, basicACL)

	cmd.Println("created:", cnr.CreatedAt())

	cmd.Println("attributes:")
	for key, val := range cnr.Attributes() {
		cmd.Printf("\t%s=%s\n", key, val)
	}

	cmd.Println("placement policy:")
	if err := cnr.PlacementPolicy().WriteStringTo((*stringWriter)(cmd)); err != nil {
		return fmt.Errorf("write policy: %w", err)
	}
	cmd.Println()
	return nil
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

func getContainer(ctx context.Context, cmd *cobra.Command) (container.Container, error) {
	var cnr container.Container
	if containerPathFrom != "" {
		data, err := os.ReadFile(containerPathFrom)
		if err != nil {
			return container.Container{}, fmt.Errorf("can't read file: %w", err)
		}

		err = cnr.Unmarshal(data)
		if err != nil {
			return container.Container{}, fmt.Errorf("can't unmarshal container: %w", err)
		}
	} else {
		id, err := parseContainerID()
		if err != nil {
			return container.Container{}, err
		}
		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return container.Container{}, err
		}
		defer cli.Close()

		cnr, err = cli.ContainerGet(ctx, id, client.PrmContainerGet{})
		if err != nil {
			return container.Container{}, fmt.Errorf("rpc error: %w", err)
		}
	}
	return cnr, nil
}
