package container

import (
	"bytes"
	"encoding/json"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	"github.com/spf13/cobra"
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
	Run: func(cmd *cobra.Command, args []string) {
		var cnr *container.Container

		if containerPathFrom != "" {
			data, err := os.ReadFile(containerPathFrom)
			common.ExitOnErr(cmd, "can't read file: %w", err)

			cnr = container.New()
			err = cnr.Unmarshal(data)
			common.ExitOnErr(cmd, "can't unmarshal container: %w", err)
		} else {
			id := parseContainerID(cmd)
			pk := key.GetOrGenerate(cmd)
			cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

			var prm internalclient.GetContainerPrm
			prm.SetClient(cli)
			prm.SetContainer(id)

			res, err := internalclient.GetContainer(prm)
			common.ExitOnErr(cmd, "rpc error: %w", err)

			cnr = res.Container()
		}

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
				data, err = cnr.Marshal()
				common.ExitOnErr(cmd, "can't binary encode container: %w", err)
			}

			err = os.WriteFile(containerPathTo, data, 0644)
			common.ExitOnErr(cmd, "can't write container to file: %w", err)
		}
	},
}

func initContainerInfoCmd() {
	commonflags.Init(getContainerInfoCmd)

	flags := getContainerInfoCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container")
	flags.StringVar(&containerPathFrom, "from", "", "path to file with encoded container")
	flags.BoolVar(&containerJSON, commonflags.JSON, false, "print or dump container in JSON format")
}

type stringWriter cobra.Command

func (x *stringWriter) WriteString(s string) (n int, err error) {
	(*cobra.Command)(x).Print(s)
	return len(s), nil
}

func prettyPrintContainer(cmd *cobra.Command, cnr *container.Container, jsonEncoding bool) {
	if cnr == nil {
		return
	}

	if jsonEncoding {
		data, err := cnr.MarshalJSON()
		if err != nil {
			common.PrintVerbose("Can't convert container to json: %w", err)
			return
		}
		buf := new(bytes.Buffer)
		if err := json.Indent(buf, data, "", "  "); err != nil {
			common.PrintVerbose("Can't pretty print json: %w", err)
		}

		cmd.Println(buf)

		return
	}

	id := container.CalculateID(cnr)
	cmd.Println("container ID:", id)

	v := cnr.Version()
	cmd.Printf("version: %d.%d\n", v.Major(), v.Minor())

	cmd.Println("owner ID:", cnr.OwnerID())

	basicACL := cnr.BasicACL()
	prettyPrintBasicACL(cmd, basicACL)

	for _, attribute := range cnr.Attributes() {
		if attribute.Key() == container.AttributeTimestamp {
			cmd.Printf("attribute: %s=%s (%s)\n",
				attribute.Key(),
				attribute.Value(),
				common.PrettyPrintUnixTime(attribute.Value()))

			continue
		}

		cmd.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}

	nonce, err := cnr.NonceUUID()
	if err == nil {
		cmd.Println("nonce:", nonce)
	} else {
		cmd.Println("invalid nonce:", err)
	}

	pp := cnr.PlacementPolicy()
	if pp == nil {
		cmd.Println("missing placement policy")
	} else {
		cmd.Println("placement policy:")
		common.ExitOnErr(cmd, "write policy: %w", pp.WriteStringTo((*stringWriter)(cmd)))
		cmd.Println()
	}
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
}
