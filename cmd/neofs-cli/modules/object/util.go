package object

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type flag[T any] struct {
	f string
	v T
}

const (
	// BearerTokenFlag is a flag for bearer token widely used across object
	// commands.
	BearerTokenFlag = "bearer"

	rawFlag     = "raw"
	rawFlagDesc = "Set raw request option"
	fileFlag    = "file"
	binaryFlag  = "binary"
)

type RPCParameters interface {
	WithBearerToken(bearer.Token)
	WithXHeaders(...string)
}

// InitBearer adds bearer token flag to a command.
func InitBearer(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String(BearerTokenFlag, "", "File with signed JSON or binary encoded bearer token")
}

// Prepare prepares object-related parameters for a command.
func Prepare(cmd *cobra.Command, prms ...RPCParameters) error {
	ttl := viper.GetUint32(commonflags.TTL)
	common.PrintVerbose(cmd, "TTL: %d", ttl)

	for i := range prms {
		btok, err := common.ReadBearerToken(cmd, BearerTokenFlag)
		if err != nil {
			return err
		}

		if btok != nil {
			prms[i].WithBearerToken(*btok)
		}
		if v, ok := prms[i].(interface{ MarkLocal() }); ok && ttl < 2 {
			v.MarkLocal()
		}
		prms[i].WithXHeaders(ParseXHeaders(cmd)...)
	}
	return nil
}

// ParseXHeaders parses comma-separated 'key=value' pairs from '--xhdr' flags.
func ParseXHeaders(cmd *cobra.Command) []string {
	xHeaders, _ := cmd.Flags().GetStringSlice(commonflags.XHeadersKey)
	xs := make([]string, 0, 2*len(xHeaders))

	for i := range xHeaders {
		k, v, found := strings.Cut(xHeaders[i], "=")
		if !found {
			panic(fmt.Errorf("invalid X-Header format: %s", xHeaders[i]))
		}

		xs = append(xs, k, v)
	}

	return xs
}

func readObjectAddress(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID) (oid.Address, error) {
	err := readCID(cmd, cnr)
	if err != nil {
		return oid.Address{}, err
	}
	err = readOID(cmd, obj)
	if err != nil {
		return oid.Address{}, err
	}

	var addr oid.Address
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr, nil
}

func readObjectAddressBin(cnr *cid.ID, obj *oid.ID, filename string) (oid.Address, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return oid.Address{}, fmt.Errorf("unable to read given file: %w", err)
	}
	objTemp := new(object.Object)
	if err := objTemp.Unmarshal(buf); err != nil {
		return oid.Address{}, fmt.Errorf("can't unmarshal object from given file: %w", err)
	}

	var addr oid.Address
	*cnr = objTemp.GetContainerID()
	*obj = objTemp.GetID()
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr, nil
}

func readCID(cmd *cobra.Command, id *cid.ID) error {
	err := id.DecodeString(cmd.Flag(commonflags.CIDFlag).Value.String())
	if err != nil {
		return fmt.Errorf("decode container ID string: %w", err)
	}
	return nil
}

func readOID(cmd *cobra.Command, id *oid.ID) error {
	if err := id.DecodeString(cmd.Flag(commonflags.OIDFlag).Value.String()); err != nil {
		return fmt.Errorf("decode object ID string: %w", err)
	}
	return nil
}

// SessionPrm is a common interface of object operation's input which supports
// sessions.
type SessionPrm interface {
	WithinSession(session.Object)
	WithinSessionV2(sessionv2.Token)
}

// forwards all parameters to _readVerifiedSession and object as nil.
func readSessionGlobal(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID) error {
	return _readVerifiedSession(cmd, dst, key, cnr, nil)
}

// forwards all parameters to _readVerifiedSession.
func readSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj oid.ID) error {
	return _readVerifiedSession(cmd, dst, key, cnr, &obj)
}

// decodes session.Object from the file by path specified in the
// commonflags.SessionToken flag. Returns nil if flag is not set.
func getSession(cmd *cobra.Command) (*session.Object, error) {
	common.PrintVerbose(cmd, "Trying to read session from the file...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "File with session token is not provided.")
		return nil, nil
	}

	common.PrintVerbose(cmd, "Reading session from the file [%s]...", path)

	var tok session.Object

	err := common.ReadBinaryOrJSON(cmd, &tok, path)
	if err != nil {
		return nil, fmt.Errorf("read session: %w", err)
	}

	return &tok, nil
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
// Supports both V1 ([session.Object]) and V2 ([sessionv2.Token]) tokens.
// V2 tokens are tried first, then falls back to V1 for backward compatibility.
//
// SessionPrm MUST be one of:
//
//	*internal.GetObjectPrm
//	*internal.HeadObjectPrm
//	*internal.SearchObjectsPrm
//	*internal.PayloadRangePrm
//	*internal.HashPayloadRangesPrm
func _readVerifiedSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) error {
	isV2, err := tryReadSessionV2(cmd, dst, key, cnr)
	if err != nil {
		return fmt.Errorf("v2 session validation failed: %w", err)
	}
	if isV2 {
		common.PrintVerbose(cmd, "Using V2 session token")
		return nil
	}

	// Fall back to V1 token
	common.PrintVerbose(cmd, "Using V1 session token")
	var cmdVerb session.ObjectVerb

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

	tok, err := getVerifiedSession(cmd, cmdVerb, key, cnr)
	if err != nil || tok == nil {
		return err
	}

	common.PrintVerbose(cmd, "Checking V1 session correctness...")

	if obj != nil && !tok.AssertObject(*obj) {
		return errors.New("unrelated object in the session")
	}

	common.PrintVerbose(cmd, "V1 session is correct.")

	dst.WithinSession(*tok)
	return nil
}

func getVerifiedSession(cmd *cobra.Command, cmdVerb session.ObjectVerb, key *ecdsa.PrivateKey, cnr cid.ID) (*session.Object, error) {
	tok, err := getSession(cmd)
	if err != nil || tok == nil {
		return tok, err
	}
	switch false {
	case tok.AssertContainer(cnr):
		return nil, errors.New("unrelated container in the session")
	case tok.AssertVerb(cmdVerb):
		return nil, errors.New("wrong verb of the session")
	case tok.AssertAuthKey((*neofsecdsa.PublicKey)(&key.PublicKey)):
		return nil, errors.New("unrelated key in the session")
	}
	if err := icrypto.AuthenticateToken(tok, nil); err != nil {
		var errScheme icrypto.ErrUnsupportedScheme
		if !errors.As(err, &errScheme) || neofscrypto.Scheme(errScheme) != neofscrypto.N3 {
			return nil, fmt.Errorf("verify session token signature: %w", err)
		}
		// CLI has no tool to verify N3 signature, so check is delegated to the server
	}
	return tok, nil
}

// ReadOrOpenSessionViaClient tries to read session from the file (V2 first, then V1),
// specified in commonflags.SessionToken flag, finalizes structures of the decoded token
// and write the result into provided SessionPrm.
// If file is missing, CreateSessionV2 is called to create V2 token.
func ReadOrOpenSessionViaClient(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, subjects []sessionv2.Target, cnr cid.ID, objs ...oid.ID) error {
	path, _ := cmd.Flags().GetString(commonflags.SessionToken)

	if path != "" {
		tokV2, err := getSessionV2(cmd)
		if err == nil && tokV2 != nil {
			return finalizeSessionV2(cmd, dst, tokV2, key, cnr)
		}

		// Fall back to V1 token from file
		tok, err := getSession(cmd)
		if err != nil {
			return err
		}
		if tok != nil {
			return finalizeSession(cmd, dst, tok, key, cnr, objs...)
		}
	}

	err := CreateSessionV2(ctx, cmd, dst, cli, key, subjects, cnr)
	if err != nil {
		return err
	}
	return nil
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
func finalizeSession(cmd *cobra.Command, dst SessionPrm, tok *session.Object, key *ecdsa.PrivateKey, cnr cid.ID, objs ...oid.ID) error {
	common.PrintVerbose(cmd, "Finalizing session token...")

	switch dst.(type) {
	default:
		panic(fmt.Sprintf("unsupported op parameters %T", dst))
	case *client.PrmObjectPutInit:
		common.PrintVerbose(cmd, "Binding session to object PUT...")
		tok.ForVerb(session.VerbObjectPut)
	case *client.PrmObjectDelete:
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
	if err != nil {
		return fmt.Errorf("sign session: %w", err)
	}

	common.PrintVerbose(cmd, "Session token successfully formed and attached to the request.")

	dst.WithinSession(*tok)
	return nil
}

// noopNNSResolver is a no-operation NNS name resolver that
// always returns that the user exists for any NNS name.
// We don't have NNS resolution in the CLI, so this resolver
// is used to skip issuer validation for NNS subjects.
type noopNNSResolver struct{}

func (r noopNNSResolver) HasUser(string, user.ID) (bool, error) {
	return true, nil
}

// CreateSessionV2 opens object session with the remote node, finalizes
// structure of the session token v2 and writes the result into the provided
// SessionPrm. Also writes provided client connection to the SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
func CreateSessionV2(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, subjects []sessionv2.Target, cnr cid.ID) error {
	const defaultTokenExp = 10 * time.Hour

	common.PrintVerbose(cmd, "Creating V2 session token locally...")
	var tok sessionv2.Token
	signer := user.NewAutoIDSigner(*key)

	currentTime := time.Now()
	tok.SetVersion(sessionv2.TokenCurrentVersion)
	// allow 10s clock skew, because time isn't synchronous over the network
	tok.SetIat(currentTime.Add(-10 * time.Second))
	tok.SetNbf(currentTime)
	tok.SetExp(currentTime.Add(defaultTokenExp))
	tok.SetFinal(true)
	err := tok.SetSubjects(subjects)
	if err != nil {
		return fmt.Errorf("set subjects: %w", err)
	}

	common.PrintVerbose(cmd, "Creating server-side session key via RPC...")

	ni, err := cli.NetworkInfo(ctx, client.PrmNetworkInfo{})
	if err != nil {
		return fmt.Errorf("get network info: %w", err)
	}
	epochInMs := int64(ni.EpochDuration()) * ni.MsPerBlock()
	if epochInMs == 0 {
		return errors.New("invalid network configuration: epoch duration is zero")
	}
	epochLifetime := (defaultTokenExp.Milliseconds() + epochInMs - 1) / epochInMs
	epochExp := ni.CurrentEpoch() + uint64(epochLifetime)
	common.PrintVerbose(cmd, "Current epoch: %d", ni.CurrentEpoch())
	common.PrintVerbose(cmd, "Token expiration epoch: %d", epochExp)

	var sessionPrm client.PrmSessionCreate
	sessionPrm.SetExp(epochExp)

	sessionRes, err := cli.SessionCreate(ctx, signer, sessionPrm)
	if err != nil {
		return fmt.Errorf("create server-side session key: %w", err)
	}

	var keySession neofsecdsa.PublicKey
	err = keySession.Decode(sessionRes.PublicKey())
	if err != nil {
		return fmt.Errorf("decode public session key: %w", err)
	}

	serverUserID := user.NewFromECDSAPublicKey((ecdsa.PublicKey)(keySession))
	err = tok.AddSubject(sessionv2.NewTargetUser(serverUserID))
	if err != nil {
		return fmt.Errorf("add server-side session key as subject: %w", err)
	}
	common.PrintVerbose(cmd, "Server-side session key created as last subject: %s", serverUserID)

	var verb sessionv2.Verb
	switch dst.(type) {
	case *client.PrmObjectPutInit:
		verb = sessionv2.VerbObjectPut
	case *client.PrmObjectDelete:
		verb = sessionv2.VerbObjectDelete
	default:
		panic(fmt.Errorf("unsupported operation type for V2 session: %T", dst))
	}

	ctx2, err := sessionv2.NewContext(cnr, []sessionv2.Verb{verb})
	if err != nil {
		return fmt.Errorf("create V2 session context: %w", err)
	}
	err = tok.SetContexts([]sessionv2.Context{ctx2})
	if err != nil {
		return fmt.Errorf("set V2 session contexts: %w", err)
	}

	if err := tok.Sign(signer); err != nil {
		return fmt.Errorf("sign V2 session: %w", err)
	}

	if err := tok.Validate(noopNNSResolver{}); err != nil {
		return fmt.Errorf("invalid V2 session token after creation: %w", err)
	}

	common.PrintVerbose(cmd, "V2 session token successfully created locally and attached to the request.")

	dst.WithinSessionV2(tok)
	return nil
}

// finalizeSessionV2 validates and attaches V2 token to the request.
func finalizeSessionV2(cmd *cobra.Command, dst SessionPrm, tok *sessionv2.Token, key *ecdsa.PrivateKey, cnr cid.ID) error {
	common.PrintVerbose(cmd, "Finalizing V2 session token...")

	if err := tok.Validate(noopNNSResolver{}); err != nil {
		return fmt.Errorf("invalid V2 session token: %w", err)
	}

	if !tok.VerifySignature() {
		return errors.New("v2 session token signature verification failed")
	}

	signer := user.NewAutoIDSigner(*key)
	if tok.Issuer() != signer.UserID() {
		return fmt.Errorf("user %s is not issuer of V2 session token (token issuer: %s)", signer.UserID(), tok.Issuer())
	}

	var verb sessionv2.Verb
	switch dst.(type) {
	case *client.PrmObjectPutInit:
		verb = sessionv2.VerbObjectPut
	case *client.PrmObjectDelete:
		verb = sessionv2.VerbObjectDelete
	default:
		panic(fmt.Errorf("unsupported operation type: %T", dst))
	}

	if !tok.AssertVerb(verb, cnr) {
		return fmt.Errorf("v2 session token does not authorize verb for container %s", cnr)
	}

	common.PrintVerbose(cmd, "V2 session token successfully validated and attached to the request.")

	dst.WithinSessionV2(*tok)
	return nil
}

// calls commonflags.InitSession with "object <verb>" name.
func initFlagSession(cmd *cobra.Command, verb string) {
	commonflags.InitSession(cmd, "object "+verb)
}

func openFileForPayload(name string) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("can't open file '%s': %w", name, err)
	}
	return f, nil
}

func parseSessionSubjects(cmd *cobra.Command, ctx context.Context, cli *client.Client) ([]sessionv2.Target, error) {
	sessionSubjects, _ := cmd.Flags().GetStringSlice(commonflags.SessionSubjectFlag)
	sessionSubjectsNNS, _ := cmd.Flags().GetStringSlice(commonflags.SessionSubjectNNSFlag)

	if len(sessionSubjects) > 0 || len(sessionSubjectsNNS) > 0 {
		common.PrintVerbose(cmd, "Using session subjects from command line flags")
		subjects := make([]sessionv2.Target, 0, len(sessionSubjects)+len(sessionSubjectsNNS))

		// Parse user IDs
		for _, subj := range sessionSubjects {
			userID, err := user.DecodeString(subj)
			if err != nil {
				return nil, fmt.Errorf("failed to decode user ID '%s': %w", subj, err)
			}
			subjects = append(subjects, sessionv2.NewTargetUser(userID))
		}

		// Parse NNS names
		for _, nnsName := range sessionSubjectsNNS {
			if nnsName == "" {
				return nil, fmt.Errorf("NNS name cannot be empty")
			}
			subjects = append(subjects, sessionv2.NewTargetNamed(nnsName))
		}

		return subjects, nil
	}

	common.PrintVerbose(cmd, "Using default session subjects (only target node)")
	res, err := cli.EndpointInfo(ctx, client.PrmEndpointInfo{})
	if err != nil {
		return nil, fmt.Errorf("get endpoint info: %w", err)
	}

	neoPubKey, err := keys.NewPublicKeyFromBytes(res.NodeInfo().PublicKey(), elliptic.P256())
	if err != nil {
		return nil, fmt.Errorf("parse node public key: %w", err)
	}

	ecdsaPubKey := (*ecdsa.PublicKey)(neoPubKey)
	userID := user.NewFromECDSAPublicKey(*ecdsaPubKey)
	return []sessionv2.Target{sessionv2.NewTargetUser(userID)}, nil
}
