package object

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"
	"strings"

	internal "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	bearerTokenFlag = "bearer"

	rawFlag     = "raw"
	rawFlagDesc = "Set raw request option"
	fileFlag    = "file"
	binaryFlag  = "binary"
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
	common.PrintVerbose(cmd, "TTL: %d", ttl)

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

func readObjectAddressBin(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID, filename string) oid.Address {
	buf, err := os.ReadFile(filename)
	common.ExitOnErr(cmd, "unable to read given file: %w", err)
	objTemp := object.New()
	common.ExitOnErr(cmd, "can't unmarshal object from given file: %w", objTemp.Unmarshal(buf))

	var addr oid.Address
	*cnr, _ = objTemp.ContainerID()
	*obj, _ = objTemp.ID()
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr
}

func readCID(cmd *cobra.Command, id *cid.ID) {
	err := id.DecodeString(cmd.Flag(commonflags.CIDFlag).Value.String())
	common.ExitOnErr(cmd, "decode container ID string: %w", err)
}

func readOID(cmd *cobra.Command, id *oid.ID) {
	err := id.DecodeString(cmd.Flag(commonflags.OIDFlag).Value.String())
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
	common.PrintVerbose(cmd, "Trying to read session from the file...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "File with session token is not provided.")
		return nil
	}

	common.PrintVerbose(cmd, "Reading session from the file [%s]...", path)

	var tok session.Object

	err := common.ReadBinaryOrJSON(cmd, &tok, path)
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

	common.PrintVerbose(cmd, "Checking session correctness...")

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

	common.PrintVerbose(cmd, "Session is correct.")

	dst.SetSessionToken(tok)
}

// ReadOrOpenSession opens client connection and calls ReadOrOpenSessionViaClient with it.
func ReadOrOpenSession(ctx context.Context, cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	cli := internal.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)
	ReadOrOpenSessionViaClient(ctx, cmd, dst, cli, key, cnr, obj)
}

// ReadOrOpenSessionViaClient tries to read session from the file specified in
// commonflags.SessionToken flag, finalizes structures of the decoded token
// and write the result into provided SessionPrm. If file is missing,
// ReadOrOpenSessionViaClient calls OpenSessionViaClient.
func ReadOrOpenSessionViaClient(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	tok := getSession(cmd)
	if tok == nil {
		OpenSessionViaClient(ctx, cmd, dst, cli, key, cnr, obj)
		return
	}

	var objs []oid.ID
	if obj != nil {
		objs = []oid.ID{*obj}
	}

	finalizeSession(cmd, dst, tok, key, cnr, objs...)
	dst.SetClient(cli)
}

// OpenSession opens client connection and calls OpenSessionViaClient with it.
func OpenSession(ctx context.Context, cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	cli := internal.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)
	OpenSessionViaClient(ctx, cmd, dst, cli, key, cnr, obj)
}

// OpenSessionViaClient opens object session with the remote node, finalizes
// structure of the session token and writes the result into the provided
// SessionPrm. Also writes provided client connection to the SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
//
// If provided SessionPrm is of type internal.DeleteObjectPrm, OpenSessionViaClient
// spreads the session to all object's relatives.
func OpenSessionViaClient(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	var objs []oid.ID

	if obj != nil {
		objs = []oid.ID{*obj}
	}

	var tok session.Object

	const sessionLifetime = 10 // in NeoFS epochs

	common.PrintVerbose(cmd, "Opening remote session with the node...")
	currEpoch, err := internal.GetCurrentEpoch(ctx, viper.GetString(commonflags.RPC))
	common.ExitOnErr(cmd, "can't fetch current epoch: %w", err)
	exp := currEpoch + sessionLifetime
	err = sessionCli.CreateSession(ctx, &tok, cli, *key, exp, currEpoch)
	common.ExitOnErr(cmd, "open remote session: %w", err)

	common.PrintVerbose(cmd, "Session successfully opened.")

	finalizeSession(cmd, dst, &tok, key, cnr, objs...)

	dst.SetClient(cli)
}

// specifies session verb, binds the session to the given container and limits
// the session by the given objects (if specified). After all data is written,
// signs session using provided private key and writes the session into the
// given SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
func finalizeSession(cmd *cobra.Command, dst SessionPrm, tok *session.Object, key *ecdsa.PrivateKey, cnr cid.ID, objs ...oid.ID) {
	common.PrintVerbose(cmd, "Finalizing session token...")

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
	case *internal.PutObjectPrm:
		common.PrintVerbose(cmd, "Binding session to object PUT...")
		tok.ForVerb(session.VerbObjectPut)
	case *internal.DeleteObjectPrm:
		common.PrintVerbose(cmd, "Binding session to object DELETE...")
		tok.ForVerb(session.VerbObjectDelete)
	}

	common.PrintVerbose(cmd, "Binding session to container %s...", cnr)

	tok.BindContainer(cnr)
	if len(objs) > 0 {
		common.PrintVerbose(cmd, "Limiting session by the objects %v...", objs)
		tok.LimitByObjects(objs...)
	}

	common.PrintVerbose(cmd, "Signing session...")

	err := tok.Sign(user.NewAutoIDSigner(*key))
	common.ExitOnErr(cmd, "sign session: %w", err)

	common.PrintVerbose(cmd, "Session token successfully formed and attached to the request.")

	dst.SetSessionToken(tok)
}

// calls commonflags.InitSession with "object <verb>" name.
func initFlagSession(cmd *cobra.Command, verb string) {
	commonflags.InitSession(cmd, "object "+verb)
}
