package object

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strings"

	internal "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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

// SessionPrm is a common interface of object operation's input which supports
// sessions.
type SessionPrm interface {
	SetSessionToken(*session.Object)
	SetClient(*client.Client)
}

// forwards all parameters to _readVerifiedSession and object as nil.
func readSessionGlobal(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID) {
	_readVerifiedSession(cmd, dst, key, cnr, nil)
}

// forwards all parameters to _readVerifiedSession.
func readSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj oid.ID) {
	_readVerifiedSession(cmd, dst, key, cnr, &obj)
}

// decodes session.Object from the file by path specified in the
// commonflags.SessionToken flag. Returns nil if flag is not set.
func getSession(cmd *cobra.Command) *session.Object {
	common.PrintVerbose("Trying to read session from the file...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose("File with session token is not provided.")
		return nil
	}

	common.PrintVerbose("Reading session from the file [%s]...", path)

	var tok session.Object

	err := common.ReadBinaryOrJSON(&tok, path)
	common.ExitOnErr(cmd, "read session: %v", err)

	return &tok
}

// decodes object session from JSON file from commonflags.SessionToken command
// flag if it is provided, and writes resulting session into the provided SessionPrm.
// Returns flag presence. Checks:
//
//   - if session verb corresponds to given SessionPrm according to its type
//   - relation to the given container
//   - relation to the given object if non-nil
//   - relation to the given private key used within the command
//   - session signature
//
// SessionPrm MUST be one of:
//
//	*internal.GetObjectPrm
//	*internal.HeadObjectPrm
//	*internal.SearchObjectsPrm
//	*internal.PayloadRangePrm
//	*internal.HashPayloadRangesPrm
func _readVerifiedSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	var cmdVerb session.ObjectVerb

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
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

	tok := getSession(cmd)
	if tok == nil {
		return
	}

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

	dst.SetSessionToken(tok)
}

// ReadOrOpenSession opens client connection and calls ReadOrOpenSessionViaClient with it.
func ReadOrOpenSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	cli := internal.GetSDKClientByFlag(cmd, key, commonflags.RPC)
	ReadOrOpenSessionViaClient(cmd, dst, cli, key, cnr, obj)
}

// ReadOrOpenSessionViaClient tries to read session from the file specified in
// commonflags.SessionToken flag, finalizes structures of the decoded token
// and write the result into provided SessionPrm. If file is missing,
// ReadOrOpenSessionViaClient calls OpenSessionViaClient.
func ReadOrOpenSessionViaClient(cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	tok := getSession(cmd)
	if tok == nil {
		OpenSessionViaClient(cmd, dst, cli, key, cnr, obj)
		return
	}

	finalizeSession(cmd, dst, tok, key, cnr, obj)
	dst.SetClient(cli)
}

// OpenSession opens client connection and calls OpenSessionViaClient with it.
func OpenSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	cli := internal.GetSDKClientByFlag(cmd, key, commonflags.RPC)
	OpenSessionViaClient(cmd, dst, cli, key, cnr, obj)
}

// OpenSessionViaClient opens object session with the remote node, finalizes
// structure of the session token and writes the result into the provided
// SessionPrm. Also writes provided client connection to the SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
func OpenSessionViaClient(cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	var tok session.Object

	const sessionLifetime = 10 // in NeoFS epochs

	common.PrintVerbose("Opening remote session with the node...")

	err := sessionCli.CreateSession(&tok, cli, sessionLifetime)
	common.ExitOnErr(cmd, "open remote session: %w", err)

	common.PrintVerbose("Session successfully opened.")

	finalizeSession(cmd, dst, &tok, key, cnr, obj)

	dst.SetClient(cli)
}

// specifies session verb, binds the session to the given container and limits
// the session by the given object (if specified). After all data is written,
// signs session using provided private key and writes the session into the
// given SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
func finalizeSession(cmd *cobra.Command, dst SessionPrm, tok *session.Object, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	common.PrintVerbose("Finalizing session token...")

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
	case *internal.PutObjectPrm:
		common.PrintVerbose("Binding session to object PUT...")
		tok.ForVerb(session.VerbObjectPut)
	case *internal.DeleteObjectPrm:
		common.PrintVerbose("Binding session to object DELETE...")
		tok.ForVerb(session.VerbObjectDelete)
	}

	common.PrintVerbose("Binding session to container %s...", cnr)

	tok.BindContainer(cnr)
	if obj != nil {
		common.PrintVerbose("Limiting session by object %s...", obj)
		tok.LimitByObjects(*obj)
	}

	common.PrintVerbose("Signing session...")

	err := tok.Sign(*key)
	common.ExitOnErr(cmd, "sign session: %w", err)

	common.PrintVerbose("Session token successfully formed and attached to the request.")

	dst.SetSessionToken(tok)
}

// calls commonflags.InitSession with "object <verb>" name.
func initFlagSession(cmd *cobra.Command, verb string) {
	commonflags.InitSession(cmd, "object "+verb)
}
