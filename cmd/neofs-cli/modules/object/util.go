package object

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strings"

	internal "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	bearerTokenFlag = "bearer"

	rawFlag     = "raw"
	rawFlagDesc = "Set raw request option"
)

type RPCParameters interface {
	SetBearerToken(prm *bearer.Token)
	SetTTL(uint32)
	SetXHeaders([]string)
}

// InitBearer adds bearer token flag to a command.
func InitBearer(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String(bearerTokenFlag, "", "File with signed JSON or binary encoded bearer token")
}

// Prepare prepares object-related parameters for a command.
func Prepare(cmd *cobra.Command, prms ...RPCParameters) {
	ttl := viper.GetUint32(commonflags.TTL)
	common.PrintVerbose("TTL: %d", ttl)

	for i := range prms {
		btok := common.ReadBearerToken(cmd, bearerTokenFlag)

		prms[i].SetBearerToken(btok)
		prms[i].SetTTL(ttl)
		prms[i].SetXHeaders(parseXHeaders(cmd))
	}
}

func parseXHeaders(cmd *cobra.Command) []string {
	xHeaders, _ := cmd.Flags().GetStringSlice(commonflags.XHeadersKey)
	xs := make([]string, 0, 2*len(xHeaders))

	for i := range xHeaders {
		kv := strings.SplitN(xHeaders[i], "=", 2)
		if len(kv) != 2 {
			panic(fmt.Errorf("invalid X-Header format: %s", xHeaders[i]))
		}

		xs = append(xs, kv[0], kv[1])
	}

	return xs
}

func readObjectAddress(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID) oid.Address {
	readCID(cmd, cnr)
	readOID(cmd, obj)

	var addr oid.Address
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr
}

func readCID(cmd *cobra.Command, id *cid.ID) {
	err := id.DecodeString(cmd.Flag("cid").Value.String())
	common.ExitOnErr(cmd, "decode container ID string: %w", err)
}

func readOID(cmd *cobra.Command, id *oid.ID) {
	err := id.DecodeString(cmd.Flag("oid").Value.String())
	common.ExitOnErr(cmd, "decode object ID string: %w", err)
}

// common interface of object operation's input which supports sessions.
// Implemented on types like internal.PutObjectPrm.
type sessionPrm interface {
	SetSessionToken(*session.Object)
}

// forwards all parameters to _readSession and object as nil.
func readSessionGlobal(cmd *cobra.Command, dst sessionPrm, key *ecdsa.PrivateKey, cnr cid.ID) {
	_readSession(cmd, dst, key, cnr, nil)
}

// forwards all parameters to _readSession.
func readSession(cmd *cobra.Command, dst sessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj oid.ID) {
	_readSession(cmd, dst, key, cnr, &obj)
}

// decodes object session from JSON file from "session" command flag if it is provided,
// and writes resulting session into the provided sessionPrm. Checks:
//
//   - if session verb corresponds to given sessionPrm according to its type
//   - relation to the given container
//   - relation to the given object if non-nil
//   - relation to the given private key used withing the command
//   - session signature
//
// sessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
//	*internal.GetObjectPrm
//	*internal.HeadObjectPrm
//	*internal.SearchObjectsPrm
//	*internal.PayloadRangePrm
//	*internal.HashPayloadRangesPrm
func _readSession(cmd *cobra.Command, dst sessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	var cmdVerb session.ObjectVerb

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
	case *internal.PutObjectPrm:
		cmdVerb = session.VerbObjectPut
	case *internal.DeleteObjectPrm:
		cmdVerb = session.VerbObjectDelete
	case *internal.GetObjectPrm:
		cmdVerb = session.VerbObjectGet
	case *internal.HeadObjectPrm:
		cmdVerb = session.VerbObjectHead
	case *internal.SearchObjectsPrm:
		cmdVerb = session.VerbObjectSearch
	case *internal.PayloadRangePrm:
		cmdVerb = session.VerbObjectRange
	case *internal.HashPayloadRangesPrm:
		cmdVerb = session.VerbObjectRangeHash
	}

	sessionTokenPath, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if sessionTokenPath != "" {
		common.PrintVerbose("Reading object session from the JSON file [%s]...", sessionTokenPath)

		var tok session.Object
		common.ReadSessionToken(cmd, &tok, sessionTokenPath)

		common.PrintVerbose("Checking session correctness...")

		switch false {
		case tok.AssertContainer(cnr):
			common.ExitOnErr(cmd, "", errors.New("unrelated container in the session"))
		case obj == nil || tok.AssertObject(*obj):
			common.ExitOnErr(cmd, "", errors.New("unrelated object in the session"))
		case tok.AssertVerb(cmdVerb):
			common.ExitOnErr(cmd, "", errors.New("wrong verb of the session"))
		case tok.AssertAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey)):
			common.ExitOnErr(cmd, "", errors.New("unrelated key in the session"))
		case tok.VerifySignature():
			common.ExitOnErr(cmd, "", errors.New("invalid signature of the session data"))
		}

		common.PrintVerbose("Session is correct.")

		dst.SetSessionToken(&tok)
	} else {
		common.PrintVerbose("Session is not provided.")
	}
}
