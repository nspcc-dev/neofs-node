package container

import (
	"context"
	"errors"
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/spf13/cobra"
)

// Set attribute command flags.
const (
	setAttributeNameFlag     = "attribute"
	setAttributeValueFlag    = "value"
	setAttributeValidForFlag = "valid-for"
)

// Set attribute command defaults.
const (
	defaultSetAttributeValidFor = time.Minute
)

var setAttributeFlagVars struct {
	id        string
	attribute string
	value     string
	validFor  time.Duration
}

var setAttributeCmd = &cobra.Command{
	Use:   "set-attribute",
	Short: "Set attribute for container",
	Long:  "Set attribute for container",
	Args:  cobra.NoArgs,
	RunE:  setAttribute,
}

func initSetAttributeCmd() {
	commonflags.Init(setAttributeCmd)

	flags := setAttributeCmd.Flags()
	flags.StringVar(&setAttributeFlagVars.id, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&setAttributeFlagVars.attribute, setAttributeNameFlag, "", "attribute to be set")
	flags.StringVar(&setAttributeFlagVars.value, setAttributeValueFlag, "", "value for the attribute")
	flags.DurationVar(&setAttributeFlagVars.validFor, setAttributeValidForFlag, defaultSetAttributeValidFor, "request validity duration")
	flags.Uint64(commonflags.ContainerRevisionFlag, 0, commonflags.ContainerRevisionFlagUsage)

	for _, f := range []string{
		commonflags.CIDFlag,
		setAttributeNameFlag,
		setAttributeValueFlag,
	} {
		if err := setAttributeCmd.MarkFlagRequired(f); err != nil {
			panic(fmt.Sprintf("failed to mark flag %s required: %v", f, err))
		}
	}
}

func setAttribute(cmd *cobra.Command, _ []string) error {
	if setAttributeFlagVars.validFor <= 0 {
		return fmt.Errorf("non-positive request validity duration %d", setAttributeFlagVars.validFor)
	}

	id, err := cid.DecodeString(setAttributeFlagVars.id)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	sessionToken, err := getSessionAnyVersion(cmd)
	if err != nil {
		return err
	}

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}
	signer := (*neofsecdsa.SignerRFC6979)(pk)

	ctx, cancel := getAwaitContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	ei, err := cli.EndpointInfo(ctx, client.PrmEndpointInfo{})
	if err != nil {
		return fmt.Errorf("fetching node info version: %w", err)
	}

	serverV := ei.LatestVersion()
	if oldContainerAttributeRPC(serverV) {
		prm := client.SetContainerAttributeParameters{
			ID:         id,
			Attribute:  setAttributeFlagVars.attribute,
			Value:      setAttributeFlagVars.value,
			ValidUntil: time.Now().Add(setAttributeFlagVars.validFor),
		}

		//nolint:staticcheck // we are the server that deprecated it, still need the support
		signedPrm := client.GetSignedSetContainerAttributeParameters(prm)

		var prmSig neofscrypto.Signature
		if err := prmSig.Calculate(signer, signedPrm); err != nil {
			return fmt.Errorf("failed to sign request parameters: %w", err)
		}

		var opts client.SetContainerAttributeOptions
		if sessionToken != nil {
			switch s := sessionToken.(type) {
			case *session.Container:
				opts.AttachSessionTokenV1(*s)
			case *sessionv2.Token:
				opts.AttachSessionToken(*s)
			default:
				panic(fmt.Sprintf("invalid session token type parsed: %T", sessionToken))
			}
		}

		//nolint:staticcheck // we are the server that deprecated it, still need the support
		err = cli.SetContainerAttribute(ctx, prm, prmSig, opts)
	} else {
		var cnrRev uint64
		cnrRev, err = fetchContainerRevision(ctx, cmd, cli, id, nil)
		if err != nil {
			return fmt.Errorf("fetching container revision: %w", err)
		}

		var opts *client.AddContainerAttributeOptions
		if sessionToken != nil {
			opts = new(client.AddContainerAttributeOptions)

			switch s := sessionToken.(type) {
			case *session.Container:
				return fmt.Errorf("trying to use v2 RPC based on server version (%s) but session V1 token is not supported", serverV)
			case *sessionv2.Token:
				if err := validateSessionV2ForContainer(cmd, s, pk, id, sessionv2.VerbContainerSetAttribute); err != nil {
					return err
				}
				opts.WithinSessionV2(*s)
			default:
				panic(fmt.Sprintf("invalid session token type parsed: %T", sessionToken))
			}
		}

		err = cli.AddContainerAttribute(ctx, signer, id, setAttributeFlagVars.attribute, setAttributeFlagVars.value, cnrRev, opts)
	}
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerAwaitTimeout) {
			err = common.ErrAwaitTimeout
		}
		return fmt.Errorf("client error: %w", err)
	}

	cmd.Println("Attribute successfully set.")

	return nil
}

// Remove attribute command flags.
const (
	removeAttributeNameFlag     = "attribute"
	removeAttributeValidForFlag = "valid-for"
)

// Remove attribute command defaults.
const (
	defaultRemoveAttributeValidFor = time.Minute
)

var removeAttributeFlagVars struct {
	id        string
	attribute string
	validFor  time.Duration
}

var removeAttributeCmd = &cobra.Command{
	Use:   "remove-attribute",
	Short: "Remove container attribute",
	Long:  "Remove container attribute",
	Args:  cobra.NoArgs,
	RunE:  removeAttribute,
}

func initRemoveAttributeCmd() {
	commonflags.Init(removeAttributeCmd)

	flags := removeAttributeCmd.Flags()
	flags.StringVar(&removeAttributeFlagVars.id, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&removeAttributeFlagVars.attribute, removeAttributeNameFlag, "", "attribute to be set")
	flags.DurationVar(&removeAttributeFlagVars.validFor, removeAttributeValidForFlag, defaultRemoveAttributeValidFor, "request validity duration")
	flags.Uint64(commonflags.ContainerRevisionFlag, 0, commonflags.ContainerRevisionFlagUsage)

	for _, f := range []string{
		commonflags.CIDFlag,
		removeAttributeNameFlag,
	} {
		if err := removeAttributeCmd.MarkFlagRequired(f); err != nil {
			panic(fmt.Sprintf("failed to mark flag %s required: %v", f, err))
		}
	}
}

func removeAttribute(cmd *cobra.Command, _ []string) error {
	if removeAttributeFlagVars.validFor <= 0 {
		return fmt.Errorf("non-positive request validity duration %d", removeAttributeFlagVars.validFor)
	}

	id, err := cid.DecodeString(removeAttributeFlagVars.id)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	sessionToken, err := getSessionAnyVersion(cmd)
	if err != nil {
		return err
	}

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}
	signer := (*neofsecdsa.SignerRFC6979)(pk)

	ctx, cancel := getAwaitContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	ei, err := cli.EndpointInfo(ctx, client.PrmEndpointInfo{})
	if err != nil {
		return fmt.Errorf("fetching node info version: %w", err)
	}
	serverV := ei.LatestVersion()
	if oldContainerAttributeRPC(serverV) {
		prm := client.RemoveContainerAttributeParameters{
			ID:         id,
			Attribute:  removeAttributeFlagVars.attribute,
			ValidUntil: time.Now().Add(removeAttributeFlagVars.validFor),
		}

		//nolint:staticcheck // we are the server that deprecated it, still need the support
		signedPrm := client.GetSignedRemoveContainerAttributeParameters(prm)

		var prmSig neofscrypto.Signature
		if err := prmSig.Calculate(signer, signedPrm); err != nil {
			return fmt.Errorf("failed to sign request parameters: %w", err)
		}

		var opts client.RemoveContainerAttributeOptions
		if sessionToken != nil {
			switch s := sessionToken.(type) {
			case *session.Container:
				opts.AttachSessionTokenV1(*s)
			case *sessionv2.Token:
				opts.AttachSessionToken(*s)
			default:
				panic(fmt.Sprintf("invalid session token type parsed: %T", sessionToken))
			}
		}

		//nolint:staticcheck // we are the server that deprecated it, still need the support
		err = cli.RemoveContainerAttribute(ctx, prm, prmSig, opts)
	} else {
		var cnrRev uint64
		cnrRev, err = fetchContainerRevision(ctx, cmd, cli, id, nil)
		if err != nil {
			return fmt.Errorf("fetching container revision: %w", err)
		}

		var opts *client.DeleteContainerAttributeOptions
		if sessionToken != nil {
			opts = new(client.DeleteContainerAttributeOptions)

			switch s := sessionToken.(type) {
			case *session.Container:
				return fmt.Errorf("trying to use v2 RPC based on server version (%s) but session V1 token is not supported", serverV)
			case *sessionv2.Token:
				if err := validateSessionV2ForContainer(cmd, s, pk, id, sessionv2.VerbContainerRemoveAttribute); err != nil {
					return err
				}
				opts.WithinSessionV2(*s)
			default:
				panic(fmt.Sprintf("invalid session token type parsed: %T", sessionToken))
			}
		}

		err = cli.DeleteContainerAttribute(ctx, signer, id, removeAttributeFlagVars.attribute, cnrRev, opts)
	}
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerAwaitTimeout) {
			err = common.ErrAwaitTimeout
		}
		return fmt.Errorf("client error: %w", err)
	}

	cmd.Println("Attribute successfully removed.")

	return nil
}

func oldContainerAttributeRPC(serverV version.Version) bool {
	return serverV.Major() < 2 || (serverV.Major() == 2 && serverV.Minor() <= 28)
}

func fetchContainerRevision(ctx context.Context, cmd *cobra.Command, cli *client.Client, id cid.ID, cnrCached *container.Container) (uint64, error) {
	var (
		cnrRev uint64
		err    error
	)
	if cmd.Flags().Changed(commonflags.ContainerRevisionFlag) {
		cnrRev, _ = cmd.Flags().GetUint64(commonflags.ContainerRevisionFlag)
		if cnrRev == 0 {
			return 0, errors.New("container revision must be positive")
		}
	} else {
		common.PrintVerbose(cmd, commonflags.ContainerRevisionFlag+" flag is not set, using latest container revision...")
		if cnrCached != nil {
			cnrRev = cnrCached.Revision()
		} else {
			cnrRev, err = getLatestContainerRevision(ctx, cli, id)
			if err != nil {
				return 0, fmt.Errorf("get latest container revision: %w", err)
			}
		}
		common.PrintVerbose(cmd, "latest container revision from the network: %d", cnrRev)
	}

	return cnrRev, nil
}
