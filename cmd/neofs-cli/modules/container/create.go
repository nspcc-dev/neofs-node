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
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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
		ctx, cancel := getAwaitContext(cmd)
		defer cancel()

		placementPolicy, err := parseContainerPolicy(cmd, containerPolicy)
		common.ExitOnErr(cmd, "", err)

		key := key.Get(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, key, commonflags.RPC)

		if !force {
			var prm internalclient.NetMapSnapshotPrm
			prm.SetClient(cli)

			resmap, err := internalclient.NetMapSnapshot(ctx, prm)
			common.ExitOnErr(cmd, "unable to get netmap snapshot to validate container placement, "+
				"use --force option to skip this check: %w", err)

			nodesByRep, err := resmap.NetMap().ContainerNodes(*placementPolicy, cid.ID{})
			common.ExitOnErr(cmd, "could not build container nodes based on given placement policy, "+
				"use --force option to skip this check: %w", err)

			for i, nodes := range nodesByRep {
				if placementPolicy.ReplicaNumberByIndex(i) > uint32(len(nodes)) {
					common.ExitOnErr(cmd, "", fmt.Errorf(
						"the number of nodes '%d' in selector is not enough for the number of replicas '%d', "+
							"use --force option to skip this check",
						len(nodes),
						placementPolicy.ReplicaNumberByIndex(i),
					))
				}
			}
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
			err = user.IDFromSigner(&idOwner, neofsecdsa.SignerRFC6979(*key))
			common.ExitOnErr(cmd, "decoding user from key", err)

			cnr.SetOwner(idOwner)
		}

		cnr.SetPlacementPolicy(*placementPolicy)
		cnr.SetBasicACL(basicACL)

		var syncContainerPrm internalclient.SyncContainerPrm
		syncContainerPrm.SetClient(cli)
		syncContainerPrm.SetContainer(&cnr)

		_, err = internalclient.SyncContainerSettings(ctx, syncContainerPrm)
		common.ExitOnErr(cmd, "syncing container's settings rpc error: %w", err)

		var putPrm internalclient.PutContainerPrm
		putPrm.SetClient(cli)
		putPrm.SetContainer(cnr)

		if tok != nil {
			putPrm.WithinSession(*tok)
		}

		res, err := internalclient.PutContainer(ctx, putPrm)
		common.ExitOnErr(cmd, "put container rpc error: %w", err)

		id := res.ID()

		cmd.Println("container creation request accepted for processing (the operation may not be completed yet)")
		cmd.Println("container ID:", id)

		if containerAwait {
			cmd.Println("awaiting...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			const waitInterval = time.Second

			t := time.NewTimer(waitInterval)
			defer t.Stop()

			for ; ; t.Reset(waitInterval) {
				select {
				case <-ctx.Done():
					common.ExitOnErr(cmd, "", errCreateTimeout)
				case <-t.C:
				}

				_, err := internalclient.GetContainer(ctx, getPrm)
				if err == nil {
					cmd.Println("container has been persisted on sidechain")
					return
				}
			}
		}
	},
}

func initContainerCreateCmd() {
	flags := createContainerCmd.Flags()

	// Init common flags
	flags.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	flags.DurationP(commonflags.Timeout, commonflags.TimeoutShorthand, commonflags.TimeoutDefault, commonflags.TimeoutUsage)
	flags.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	flags.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)

	flags.StringVar(&containerACL, "basic-acl", acl.NamePrivate, fmt.Sprintf("HEX encoded basic ACL value or keywords like '%s', '%s', '%s'",
		acl.NamePublicRW, acl.NamePrivate, acl.NamePublicROExtended,
	))
	flags.StringVarP(&containerPolicy, "policy", "p", "", "QL-encoded or JSON-encoded placement policy or path to file with it")
	flags.StringSliceVarP(&containerAttributes, "attributes", "a", nil, "Comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2")
	flags.BoolVar(&containerAwait, "await", false, fmt.Sprintf("Block execution until container is persisted. "+
		"Increases default execution timeout to %.0fs", awaitTimeout.Seconds())) // simple %s notation prints 1m0s https://github.com/golang/go/issues/39064
	flags.StringVar(&containerName, "name", "", "Container name attribute")
	flags.BoolVar(&containerNoTimestamp, "disable-timestamp", false, "Disable timestamp container attribute")
	flags.StringVar(&containerSubnet, "subnet", "", "String representation of container subnetwork")
	flags.BoolVarP(&force, commonflags.ForceFlag, commonflags.ForceFlagShorthand, false,
		"Skip placement validity check")
}

func parseContainerPolicy(cmd *cobra.Command, policyString string) (*netmap.PlacementPolicy, error) {
	_, err := os.Stat(policyString) // check if `policyString` is a path to file with placement policy
	if err == nil {
		common.PrintVerbose(cmd, "Reading placement policy from file: %s", policyString)

		data, err := os.ReadFile(policyString)
		if err != nil {
			return nil, fmt.Errorf("can't read file with placement policy: %w", err)
		}

		policyString = string(data)
	}

	var result netmap.PlacementPolicy

	err = result.DecodeString(policyString)
	if err == nil {
		common.PrintVerbose(cmd, "Parsed QL encoded policy")
		return &result, nil
	}

	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		common.PrintVerbose(cmd, "Parsed JSON encoded policy")
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
