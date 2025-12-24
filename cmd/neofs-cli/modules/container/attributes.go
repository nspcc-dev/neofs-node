package container

import (
	"errors"
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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

	sessionToken, err := getSession(cmd)
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

	prm := client.SetContainerAttributeParameters{
		ID:         id,
		Attribute:  setAttributeFlagVars.attribute,
		Value:      setAttributeFlagVars.value,
		ValidUntil: time.Now().Add(setAttributeFlagVars.validFor),
	}

	signedPrm := client.GetSignedSetContainerAttributeParameters(prm)

	var prmSig neofscrypto.Signature
	if err := prmSig.Calculate(signer, signedPrm); err != nil {
		return fmt.Errorf("failed to sign request parameters: %w", err)
	}

	var opts client.SetContainerAttributeOptions
	if sessionToken != nil {
		opts.AttachSessionTokenV1(*sessionToken)
	}

	err = cli.SetContainerAttribute(ctx, prm, prmSig, opts)
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

	sessionToken, err := getSession(cmd)
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

	prm := client.RemoveContainerAttributeParameters{
		ID:         id,
		Attribute:  removeAttributeFlagVars.attribute,
		ValidUntil: time.Now().Add(removeAttributeFlagVars.validFor),
	}

	signedPrm := client.GetSignedRemoveContainerAttributeParameters(prm)

	var prmSig neofscrypto.Signature
	if err := prmSig.Calculate(signer, signedPrm); err != nil {
		return fmt.Errorf("failed to sign request parameters: %w", err)
	}

	var opts client.RemoveContainerAttributeOptions
	if sessionToken != nil {
		opts.AttachSessionTokenV1(*sessionToken)
	}

	err = cli.RemoveContainerAttribute(ctx, prm, prmSig, opts)
	if err != nil {
		if errors.Is(err, apistatus.ErrContainerAwaitTimeout) {
			err = common.ErrAwaitTimeout
		}
		return fmt.Errorf("client error: %w", err)
	}

	cmd.Println("Attribute successfully removed.")

	return nil
}
