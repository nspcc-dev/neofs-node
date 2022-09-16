package session

import (
	"crypto/ecdsa"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

// RPCParameters represents parameters for operations with session token.
type RPCParameters interface {
	SetClient(*client.Client)
	SetSessionToken(*session.Object)
}

const sessionTokenLifetime = 10 // in epochs

// Prepare prepares session for a command.
func Prepare(cmd *cobra.Command, cnr cid.ID, obj *oid.ID, key *ecdsa.PrivateKey, prms ...RPCParameters) {
	cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

	var tok session.Object
	if tokenPath, _ := cmd.Flags().GetString(commonflags.SessionToken); len(tokenPath) != 0 {
		data, err := os.ReadFile(tokenPath)
		common.ExitOnErr(cmd, "can't read session token: %w", err)

		if err := tok.Unmarshal(data); err != nil {
			err = tok.UnmarshalJSON(data)
			common.ExitOnErr(cmd, "can't unmarshal session token: %w", err)
		}
	} else {
		err := CreateSession(&tok, cli, sessionTokenLifetime)
		common.ExitOnErr(cmd, "create session: %w", err)
	}

	for i := range prms {
		tok := tok
		switch prms[i].(type) {
		case *internalclient.GetObjectPrm:
			tok.ForVerb(session.VerbObjectGet)
		case *internalclient.HeadObjectPrm:
			tok.ForVerb(session.VerbObjectHead)
		case *internalclient.PutObjectPrm:
			tok.ForVerb(session.VerbObjectPut)
		case *internalclient.DeleteObjectPrm:
			tok.ForVerb(session.VerbObjectDelete)
		case *internalclient.SearchObjectsPrm:
			tok.ForVerb(session.VerbObjectSearch)
		case *internalclient.PayloadRangePrm:
			tok.ForVerb(session.VerbObjectRange)
		case *internalclient.HashPayloadRangesPrm:
			tok.ForVerb(session.VerbObjectRangeHash)
		default:
			panic("invalid client parameter type")
		}

		tok.BindContainer(cnr)
		if obj != nil {
			tok.LimitByObjects(*obj)
		}

		err := tok.Sign(*key)
		common.ExitOnErr(cmd, "session token signing: %w", err)

		prms[i].SetClient(cli)
		prms[i].SetSessionToken(&tok)
	}
}
