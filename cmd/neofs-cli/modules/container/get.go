package container

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
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
	flags.BoolVar(&containerJSON, "json", false, "print or dump container in JSON format")
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
	prettyPrintBasicACL(cmd, acl.BasicACL(basicACL))

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

	cmd.Println("placement policy:")
	cmd.Println(strings.Join(policy.Encode(cnr.PlacementPolicy()), "\n"))
}

func prettyPrintBasicACL(cmd *cobra.Command, basicACL acl.BasicACL) {
	cmd.Printf("basic ACL: %s", basicACL)
	for k, v := range wellKnownBasicACL {
		if v == basicACL {
			cmd.Printf(" (%s)\n", k)
			return
		}
	}
	cmd.Println()
}
