package object

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

func getSessionV2(cmd *cobra.Command) (*session.Token, error) {
	common.PrintVerbose(cmd, "Trying to read V2 session from the file...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "File with session token is not provided.")
		return nil, nil
	}

	common.PrintVerbose(cmd, "Reading V2 session from the file [%s]...", path)

	var tok session.Token

	err := common.ReadBinaryOrJSON(cmd, &tok, path)
	if err != nil {
		return nil, fmt.Errorf("read V2 session: %w", err)
	}

	return &tok, nil
}

func getVerifiedSessionV2(cmd *cobra.Command, cmdVerb session.Verb, key *ecdsa.PrivateKey, cnr cid.ID) (*session.Token, error) {
	tok, err := getSessionV2(cmd)
	if err != nil || tok == nil {
		return tok, err
	}

	common.PrintVerbose(cmd, "Validating V2 session token...")

	if err := tok.Validate(); err != nil {
		return nil, fmt.Errorf("invalid V2 session token: %w", err)
	}

	if !tok.AssertVerb(cmdVerb, cnr) {
		return nil, fmt.Errorf("v2 session token does not authorize %v for container %s", cmdVerb, cnr)
	}

	signer := user.NewAutoIDSigner(*key)
	if tok.Issuer() != signer.UserID() {
		return nil, fmt.Errorf("v2 session token issuer %v does not match provided key/wallet %s", tok.Issuer(), signer.UserID())
	}

	common.PrintVerbose(cmd, "V2 session token validated successfully")
	return tok, nil
}

func _readVerifiedSessionV2(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) error {
	var cmdVerb session.Verb

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
	case *client.PrmObjectGet:
		cmdVerb = session.VerbObjectGet
	case *client.PrmObjectHead:
		cmdVerb = session.VerbObjectHead
	case *client.PrmObjectSearch:
		cmdVerb = session.VerbObjectSearch
	case *client.PrmObjectRange:
		cmdVerb = session.VerbObjectRange
	case *client.PrmObjectHash:
		cmdVerb = session.VerbObjectRangeHash
	}

	tok, err := getVerifiedSessionV2(cmd, cmdVerb, key, cnr)
	if err != nil || tok == nil {
		return err
	}

	common.PrintVerbose(cmd, "Checking V2 session correctness...")

	if obj != nil {
		if !tok.AssertObject(cmdVerb, cnr, *obj) {
			return fmt.Errorf("v2 session token does not authorize access to object %s", obj)
		}
	}

	common.PrintVerbose(cmd, "V2 session is correct.")

	dst.WithinSessionV2(*tok)
	return nil
}

func tryReadSessionV2(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) (bool, error) {
	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		return false, nil
	}

	tok, err := getSessionV2(cmd)
	if err != nil {
		return false, nil
	}
	if tok == nil {
		return false, nil
	}

	err = _readVerifiedSessionV2(cmd, dst, key, cnr, obj)
	if err != nil {
		return true, err
	}

	return true, nil
}
