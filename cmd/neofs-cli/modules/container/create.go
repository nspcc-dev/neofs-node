package container

import (
	"errors"
	"fmt"
	"strings"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	containerpolicy "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/container/policy"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
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

		placementPolicy, err := containerpolicy.ParseContainerPolicy(cmd, containerPolicy)
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

			nodeSets, err := nm.ContainerNodes(*placementPolicy, cid.ID{})
			if err != nil {
				return fmt.Errorf("could not build container nodes based on given placement policy, "+
					"use --force option to skip this check: %w", err)
			}

			repRuleNum := placementPolicy.NumberOfReplicas()
			for i := range repRuleNum {
				if placementPolicy.ReplicaNumberByIndex(i) > uint32(len(nodeSets[i])) {
					return fmt.Errorf(
						"the number of nodes '%d' in selector is not enough for the number of replicas '%d', "+
							"use --force option to skip this check",
						len(nodeSets[i]),
						placementPolicy.ReplicaNumberByIndex(i),
					)
				}
			}

			ecRules := placementPolicy.ECRules()
			for i := range ecRules {
				d := ecRules[i].DataPartNum()
				p := ecRules[i].ParityPartNum()
				n := uint32(len(nodeSets[repRuleNum+i]))
				if d > n || p > n || d+p > n {
					return fmt.Errorf(
						"the number of nodes '%d' in selector is not enough for EC rule '%d/%d', "+
							"use --force option to skip this check", n, d, p)
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

		tokAny, err := getSessionAnyVersion(cmd)
		if err != nil {
			return err
		}

		if tokAny != nil {
			switch tok := tokAny.(type) {
			case *sessionv2.Token:
				cnr.SetOwner(tok.OriginalIssuer())
			case *session.Container:
				cnr.SetOwner(tok.Issuer())
			default:
				return errors.New("unsupported session token type")
			}
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
		if tokAny != nil {
			switch tok := tokAny.(type) {
			case *sessionv2.Token:
				if err := validateSessionV2ForContainer(cmd, tok, key, cid.ID{}, sessionv2.VerbContainerPut); err != nil {
					return err
				}
				putPrm.WithinSessionV2(*tok)
			case *session.Container:
				putPrm.WithinSession(*tok)
			}
		}

		id, err := actor.ContainerPut(ctx, cnr, user.NewAutoIDSignerRFC6979(*key), putPrm)
		if err != nil {
			if errors.Is(err, apistatus.ErrContainerAwaitTimeout) {
				err = waiter.ErrConfirmationTimeout
			}
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

func parseAttributes(dst *container.Container, attributes []string) error {
	for i := range attributes {
		k, v, found := strings.Cut(attributes[i], attributeDelimiter)
		if !found {
			return errors.New("invalid container attribute")
		}

		if k == "Timestamp" && !containerNoTimestamp {
			return errors.New("can't override default timestamp attribute, use '--disable-timestamp' flag")
		}

		if k == "Name" && containerName != "" && v != containerName {
			return errors.New("name from the '--name' flag and the 'Name' attribute are not equal, " +
				"you need to use one of them or make them equal")
		}

		if k == "" {
			return errors.New("empty attribute key")
		} else if v == "" {
			return fmt.Errorf("empty attribute value for key %s", k)
		}

		dst.SetAttribute(k, v)
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
