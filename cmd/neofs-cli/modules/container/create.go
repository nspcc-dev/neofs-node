package container

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var (
	containerACL         string
	containerPolicy      string
	containerAttributes  []string
	containerAwait       bool
	containerName        string
	containerNoTimestamp bool
	containerSubnet      string
	force                bool
)

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS. 
It will be stored in sidechain when inner ring will accepts it.`,
	Run: func(cmd *cobra.Command, args []string) {
		placementPolicy, err := parseContainerPolicy(containerPolicy)
		common.ExitOnErr(cmd, "", err)

		key := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

		if !force {
			var prm internalclient.NetMapSnapshotPrm
			prm.SetClient(cli)

			resmap, err := internalclient.NetMapSnapshot(prm)
			common.ExitOnErr(cmd, "unable to get netmap snapshot to validate container placement, "+
				"use --force option to skip this check: %w", err)

			_, err = resmap.NetMap().ContainerNodes(*placementPolicy, nil)
			common.ExitOnErr(cmd, "could not build container nodes based on given placement policy, "+
				"use --force option to skip this check: %w", err)
		}

		if containerSubnet != "" {
			var subnetID subnetid.ID

			err = subnetID.DecodeString(containerSubnet)
			common.ExitOnErr(cmd, "could not parse subnetID: %w", err)

			placementPolicy.RestrictSubnet(subnetID)
		}

		var cnr container.Container
		cnr.Init()

		err = parseAttributes(&cnr, containerAttributes)
		common.ExitOnErr(cmd, "", err)

		var basicACL acl.Basic
		common.ExitOnErr(cmd, "decode basic ACL string: %w", basicACL.DecodeString(containerACL))

		tok := getSession(cmd)

		if tok != nil {
			issuer := tok.Issuer()
			cnr.SetOwner(issuer)
		} else {
			var idOwner user.ID
			user.IDFromKey(&idOwner, key.PublicKey)

			cnr.SetOwner(idOwner)
		}

		cnr.SetPlacementPolicy(*placementPolicy)
		cnr.SetBasicACL(basicACL)

		var syncContainerPrm internalclient.SyncContainerPrm
		syncContainerPrm.SetClient(cli)
		syncContainerPrm.SetContainer(&cnr)

		_, err = internalclient.SyncContainerSettings(syncContainerPrm)
		common.ExitOnErr(cmd, "syncing container's settings rpc error: %w", err)

		var putPrm internalclient.PutContainerPrm
		putPrm.SetClient(cli)
		putPrm.SetContainer(cnr)

		if tok != nil {
			putPrm.WithinSession(*tok)
		}

		res, err := internalclient.PutContainer(putPrm)
		common.ExitOnErr(cmd, "put container rpc error: %w", err)

		id := res.ID()

		cmd.Println("container ID:", id)

		if containerAwait {
			cmd.Println("awaiting...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := internalclient.GetContainer(getPrm)
				if err == nil {
					cmd.Println("container has been persisted on sidechain")
					return
				}
			}

			common.ExitOnErr(cmd, "", errCreateTimeout)
		}
	},
}

func initContainerCreateCmd() {
	commonflags.Init(createContainerCmd)

	flags := createContainerCmd.Flags()

	flags.StringVar(&containerACL, "basic-acl", acl.NamePrivate, fmt.Sprintf("HEX encoded basic ACL value or keywords like '%s', '%s', '%s'",
		acl.NamePublicRW, acl.NamePrivate, acl.NamePublicROExtended,
	))
	flags.StringVarP(&containerPolicy, "policy", "p", "", "QL-encoded or JSON-encoded placement policy or path to file with it")
	flags.StringSliceVarP(&containerAttributes, "attributes", "a", nil, "Comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2")
	flags.BoolVar(&containerAwait, "await", false, "Block execution until container is persisted")
	flags.StringVar(&containerName, "name", "", "Container name attribute")
	flags.BoolVar(&containerNoTimestamp, "disable-timestamp", false, "Disable timestamp container attribute")
	flags.StringVar(&containerSubnet, "subnet", "", "String representation of container subnetwork")
	flags.BoolVarP(&force, commonflags.ForceFlag, commonflags.ForceFlagShorthand, false,
		"Skip placement validity check")
}

func parseContainerPolicy(policyString string) (*netmap.PlacementPolicy, error) {
	_, err := os.Stat(policyString) // check if `policyString` is a path to file with placement policy
	if err == nil {
		common.PrintVerbose("Reading placement policy from file: %s", policyString)

		data, err := os.ReadFile(policyString)
		if err != nil {
			return nil, fmt.Errorf("can't read file with placement policy: %w", err)
		}

		policyString = string(data)
	}

	var result netmap.PlacementPolicy

	err = result.DecodeString(policyString)
	if err == nil {
		common.PrintVerbose("Parsed QL encoded policy")
		return &result, nil
	}

	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		common.PrintVerbose("Parsed JSON encoded policy")
		return &result, nil
	}

	return nil, errors.New("can't parse placement policy")
}

func parseAttributes(dst *container.Container, attributes []string) error {
	for i := range attributes {
		kvPair := strings.Split(attributes[i], attributeDelimiter)
		if len(kvPair) != 2 {
			return errors.New("invalid container attribute")
		}

		dst.SetAttribute(kvPair[0], kvPair[1])
	}

	if !containerNoTimestamp {
		container.SetCreationTime(dst, time.Now())
	}

	if containerName != "" {
		container.SetName(dst, containerName)
	}

	return nil
}
