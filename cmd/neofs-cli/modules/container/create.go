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
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/waiter"
	"github.com/spf13/cobra"
)

var (
	containerACL         string
	containerPolicy      string
	containerAttributes  []string
	containerAwait       bool
	containerName        string
	containerNoTimestamp bool
	force                bool
	containerGlobalName  bool
)

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS.
It will be stored in FS chain when inner ring will accepts it.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if containerName == "" && containerGlobalName {
			return errors.New("--global-name requires a name attribute")
		}

		placementPolicy, err := parseContainerPolicy(cmd, containerPolicy)
		if err != nil {
			return err
		}

		key, err := key.Get(cmd)
		if err != nil {
			return err
		}

		ctx, cancel := getAwaitContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		if !force {
			nm, err := cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
			if err != nil {
				return fmt.Errorf("unable to get netmap snapshot to validate container placement, "+
					"use --force option to skip this check: %w", err)
			}

			nodesByRep, err := nm.ContainerNodes(*placementPolicy, cid.ID{})
			if err != nil {
				return fmt.Errorf("could not build container nodes based on given placement policy, "+
					"use --force option to skip this check: %w", err)
			}

			for i, nodes := range nodesByRep {
				if placementPolicy.ReplicaNumberByIndex(i) > uint32(len(nodes)) {
					return fmt.Errorf(
						"the number of nodes '%d' in selector is not enough for the number of replicas '%d', "+
							"use --force option to skip this check",
						len(nodes),
						placementPolicy.ReplicaNumberByIndex(i),
					)
				}
			}
		}

		var cnr container.Container
		cnr.Init()

		err = parseAttributes(&cnr, containerAttributes)
		if err != nil {
			return err
		}

		var basicACL acl.Basic
		if err := basicACL.DecodeString(containerACL); err != nil {
			return fmt.Errorf("decode basic ACL string: %w", err)
		}

		tok, err := getSession(cmd)
		if err != nil {
			return err
		}

		if tok != nil {
			issuer := tok.Issuer()
			cnr.SetOwner(issuer)
		} else {
			cnr.SetOwner(user.NewFromECDSAPublicKey(key.PublicKey))
		}

		cnr.SetPlacementPolicy(*placementPolicy)
		cnr.SetBasicACL(basicACL)

		ni, err := cli.NetworkInfo(ctx, client.PrmNetworkInfo{})
		if err != nil {
			return fmt.Errorf("fetching network info: %w", err)
		}
		cnr.ApplyNetworkConfig(ni)

		var actor containerModifier = cli

		if containerAwait {
			actor = waiter.NewWaiter(cli, pollTimeFromNetworkInfo(ni))
		}

		var putPrm client.PrmContainerPut
		if tok != nil {
			putPrm.WithinSession(*tok)
		}

		id, err := actor.ContainerPut(ctx, cnr, user.NewAutoIDSignerRFC6979(*key), putPrm)
		if err != nil {
			return fmt.Errorf("put container rpc error: %w", err)
		}

		if !containerAwait {
			cmd.Println("container creation request accepted for processing (the operation may not be completed yet)")
		} else {
			cmd.Println("container has been persisted on FS chain")
		}
		cmd.Println("container ID:", id)

		return nil
	},
}

func initContainerCreateCmd() {
	flags := createContainerCmd.Flags()

	// Init common flags
	flags.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	flags.DurationP(commonflags.Timeout, commonflags.TimeoutShorthand, commonflags.TimeoutDefault, commonflags.TimeoutUsage)
	flags.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	flags.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	flags.StringVar(&containerACL, "basic-acl", acl.NamePrivate, fmt.Sprintf("HEX-encoded basic ACL value or one of the keywords ['%s', '%s', '%s','%s', '%s', '%s', '%s', '%s']. To see the basic ACL details, run: 'neofs-cli acl basic print'",
		acl.NamePublicRW, acl.NamePrivate, acl.NamePublicROExtended, acl.NamePrivateExtended, acl.NamePublicRO, acl.NamePublicRWExtended, acl.NamePublicAppend, acl.NamePublicAppendExtended))
	flags.StringVarP(&containerPolicy, "policy", "p", "", "QL-encoded or JSON-encoded placement policy or path to file with it")
	flags.StringSliceVarP(&containerAttributes, "attributes", "a", nil, "Comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2")
	flags.BoolVar(&containerAwait, "await", false, fmt.Sprintf("Block execution until container is persisted. "+
		"Increases default execution timeout to %.0fs", awaitTimeout.Seconds())) // simple %s notation prints 1m0s https://github.com/golang/go/issues/39064
	flags.StringVar(&containerName, "name", "", "Container name attribute")
	flags.BoolVar(&containerNoTimestamp, "disable-timestamp", false, "Disable timestamp container attribute")
	flags.BoolVarP(&force, commonflags.ForceFlag, commonflags.ForceFlagShorthand, false,
		"Skip placement validity check")
	flags.BoolVar(&containerGlobalName, "global-name", false, "Name becomes a domain name, that is registered with the default zone in NNS contract. Requires name attribute.")
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

		if kvPair[0] == "Timestamp" && !containerNoTimestamp {
			return errors.New("can't override default timestamp attribute, use '--disable-timestamp' flag")
		}

		if kvPair[0] == "Name" && containerName != "" && kvPair[1] != containerName {
			return errors.New("name from the '--name' flag and the 'Name' attribute are not equal, " +
				"you need to use one of them or make them equal")
		}

		if kvPair[0] == "" {
			return errors.New("empty attribute key")
		} else if kvPair[1] == "" {
			return fmt.Errorf("empty attribute value for key %s", kvPair[0])
		}

		dst.SetAttribute(kvPair[0], kvPair[1])
	}

	if !containerNoTimestamp {
		dst.SetCreationTime(time.Now())
	}

	if containerName != "" {
		dst.SetName(containerName)

		if containerGlobalName {
			var d container.Domain
			d.SetName(containerName)
			dst.WriteDomain(d)
		}
	}

	return nil
}
