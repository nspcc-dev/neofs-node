package container

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

func getSessionV2(cmd *cobra.Command) (*session.TokenV2, error) {
	common.PrintVerbose(cmd, "Trying to read V2 container session from the file...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "Session not provided.")
		return nil, nil
	}

	common.PrintVerbose(cmd, "Reading V2 container session from the file [%s]...", path)

	var tok session.TokenV2

	err := common.ReadBinaryOrJSON(cmd, &tok, path)
	if err != nil {
		return nil, fmt.Errorf("read V2 container session: %w", err)
	}

	common.PrintVerbose(cmd, "V2 session successfully read.")
	return &tok, nil
}

// getSessionAnyVersion tries V2 token first, then V1.
func getSessionAnyVersion(cmd *cobra.Command) (any, error) {
	tokV2, err := getSessionV2(cmd)
	if err == nil && tokV2 != nil {
		return tokV2, nil
	}

	tok, err := getSession(cmd)
	if err != nil {
		return nil, err
	}
	if tok != nil {
		return tok, nil
	}

	return nil, nil
}

// validateSessionV2ForContainer validates V2 token for container operations.
func validateSessionV2ForContainer(cmd *cobra.Command, tok *session.TokenV2, key *ecdsa.PrivateKey, cnrID cid.ID, verb session.VerbV2) error {
	common.PrintVerbose(cmd, "Validating V2 session token...")

	if err := tok.Validate(); err != nil {
		return fmt.Errorf("invalid V2 session token: %w", err)
	}

	if !tok.VerifySignature() {
		return errors.New("v2 session token signature verification failed")
	}

	signer := user.NewAutoIDSigner(*key)
	target := session.NewTarget(signer.UserID())

	if !tok.Issuer().Equals(target) {
		return fmt.Errorf("V2 session token issuer %v does not match user %s", tok.Issuer(), signer.UserID().String())
	}

	if !tok.AssertContainer(verb, cnrID) {
		return fmt.Errorf("v2 session token does not authorize %v for container %s", verb, cnrID.String())
	}

	common.PrintVerbose(cmd, "V2 session token validated successfully")
	return nil
}
