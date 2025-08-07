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
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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
	SetBearerToken(prm *bearer.Token)
	SetTTL(uint32)
	SetXHeaders([]string)
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

		prms[i].SetBearerToken(btok)
		prms[i].SetTTL(ttl)
		prms[i].SetXHeaders(ParseXHeaders(cmd))
	}
	return nil
}

// ParseXHeaders parses comma-separated 'key=value' pairs from '--xhdr' flags.
func ParseXHeaders(cmd *cobra.Command) []string {
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
	objTemp := object.New()
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
// SessionPrm MUST be one of:
//
//	*internal.GetObjectPrm
//	*internal.HeadObjectPrm
//	*internal.SearchObjectsPrm
//	*internal.PayloadRangePrm
//	*internal.HashPayloadRangesPrm
func _readVerifiedSession(cmd *cobra.Command, dst SessionPrm, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) error {
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

	common.PrintVerbose(cmd, "Checking session correctness...")

	if obj != nil && !tok.AssertObject(*obj) {
		return errors.New("unrelated object in the session")
	}

	common.PrintVerbose(cmd, "Session is correct.")

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

// ReadOrOpenSessionViaClient tries to read session from the file specified in
// commonflags.SessionToken flag, finalizes structures of the decoded token
// and write the result into provided SessionPrm. If file is missing,
// ReadOrOpenSessionViaClient calls OpenSessionViaClient.
func ReadOrOpenSessionViaClient(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, objs ...oid.ID) error {
	tok, err := getSession(cmd)
	if err != nil {
		return err
	}
	if tok == nil {
		err = OpenSessionViaClient(ctx, cmd, dst, cli, key, cnr, objs...)
		if err != nil {
			return err
		}
		return nil
	}

	err = finalizeSession(cmd, dst, tok, key, cnr, objs...)
	if err != nil {
		return err
	}
	return nil
}

// OpenSessionViaClient opens object session with the remote node, finalizes
// structure of the session token and writes the result into the provided
// SessionPrm. Also writes provided client connection to the SessionPrm.
//
// SessionPrm MUST be one of:
//
//	*internal.PutObjectPrm
//	*internal.DeleteObjectPrm
func OpenSessionViaClient(ctx context.Context, cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, objs ...oid.ID) error {
	var tok session.Object

	const sessionLifetime = 10 // in NeoFS epochs

	common.PrintVerbose(cmd, "Opening remote session with the node...")
	currEpoch, err := internal.GetCurrentEpoch(ctx, viper.GetString(commonflags.RPC))
	if err != nil {
		return fmt.Errorf("can't fetch current epoch: %w", err)
	}
	exp := currEpoch + sessionLifetime
	err = sessionCli.CreateSession(ctx, &tok, cli, *key, exp, currEpoch)
	if err != nil {
		return fmt.Errorf("open remote session: %w", err)
	}

	common.PrintVerbose(cmd, "Session successfully opened.")

	err = finalizeSession(cmd, dst, &tok, key, cnr, objs...)
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

// calls commonflags.InitSession with "object <verb>" name.
func initFlagSession(cmd *cobra.Command, verb string) {
	commonflags.InitSession(cmd, "object "+verb)
}
