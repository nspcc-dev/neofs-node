package object

import (
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

	var objs []oid.ID
	if obj != nil {
		objs = []oid.ID{*obj}

		if _, ok := dst.(*internal.DeleteObjectPrm); ok {
			common.PrintVerbose(cmd, "Collecting relatives of the removal object...")

			objs = append(objs, collectObjectRelatives(cmd, cli, cnr, *obj)...)
		}
	}

	finalizeSession(cmd, dst, tok, key, cnr, objs...)
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
//
// If provided SessionPrm is of type internal.DeleteObjectPrm, OpenSessionViaClient
// spreads the session to all object's relatives.
func OpenSessionViaClient(cmd *cobra.Command, dst SessionPrm, cli *client.Client, key *ecdsa.PrivateKey, cnr cid.ID, obj *oid.ID) {
	var objs []oid.ID

	if obj != nil {
		if _, ok := dst.(*internal.DeleteObjectPrm); ok {
			common.PrintVerbose(cmd, "Collecting relatives of the removal object...")

			rels := collectObjectRelatives(cmd, cli, cnr, *obj)

			if len(rels) == 0 {
				objs = []oid.ID{*obj}
			} else {
				objs = append(rels, *obj)
			}
		}
	}

	var tok session.Object

	const sessionLifetime = 10 // in NeoFS epochs

	common.PrintVerbose(cmd, "Opening remote session with the node...")

	err := sessionCli.CreateSession(&tok, cli, sessionLifetime)
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

	err := tok.Sign(neofsecdsa.SignerRFC6979(*key))
	common.ExitOnErr(cmd, "sign session: %w", err)

	common.PrintVerbose(cmd, "Session token successfully formed and attached to the request.")

	dst.SetSessionToken(tok)
}

// calls commonflags.InitSession with "object <verb>" name.
func initFlagSession(cmd *cobra.Command, verb string) {
	commonflags.InitSession(cmd, "object "+verb)
}

// collects and returns all relatives of the given object stored in the specified
// container. Empty result without an error means lack of relationship in the
// container.
//
// The object itself is not included in the result.
func collectObjectRelatives(cmd *cobra.Command, cli *client.Client, cnr cid.ID, obj oid.ID) []oid.ID {
	common.PrintVerbose(cmd, "Fetching raw object header...")

	// request raw header first
	var addrObj oid.Address
	addrObj.SetContainer(cnr)
	addrObj.SetObject(obj)

	var prmHead internal.HeadObjectPrm
	prmHead.SetClient(cli)
	prmHead.SetAddress(addrObj)
	prmHead.SetRawFlag(true)

	Prepare(cmd, &prmHead)

	_, err := internal.HeadObject(prmHead)

	var errSplit *object.SplitInfoError

	switch {
	default:
		common.ExitOnErr(cmd, "failed to get raw object header: %w", err)
	case err == nil:
		common.PrintVerbose(cmd, "Raw header received - object is singular.")
		return nil
	case errors.As(err, &errSplit):
		common.PrintVerbose(cmd, "Split information received - object is virtual.")
	}

	splitInfo := errSplit.SplitInfo()

	// collect split chain by the descending ease of operations (ease is evaluated heuristically).
	// If any approach fails, we don't try the next since we assume that it will fail too.

	if idLinking, ok := splitInfo.Link(); ok {
		common.PrintVerbose(cmd, "Collecting split members using linking object %s...", idLinking)

		addrObj.SetObject(idLinking)
		prmHead.SetAddress(addrObj)
		prmHead.SetRawFlag(false)
		// client is already set

		res, err := internal.HeadObject(prmHead)
		if err == nil {
			children := res.Header().Children()

			common.PrintVerbose(cmd, "Received split members from the linking object: %v", children)

			// include linking object
			return append(children, idLinking)
		}

		// linking object is not required for
		// object collecting
		common.PrintVerbose(cmd, "failed to get linking object's header: %w", err)
	}

	if idSplit := splitInfo.SplitID(); idSplit != nil {
		common.PrintVerbose(cmd, "Collecting split members by split ID...")

		var query object.SearchFilters
		query.AddSplitIDFilter(object.MatchStringEqual, idSplit)

		var prm internal.SearchObjectsPrm
		prm.SetContainerID(cnr)
		prm.SetClient(cli)
		prm.SetFilters(query)

		res, err := internal.SearchObjects(prm)
		common.ExitOnErr(cmd, "failed to search objects by split ID: %w", err)

		members := res.IDList()

		common.PrintVerbose(cmd, "Found objects by split ID: %v", res.IDList())

		return members
	}

	idMember, ok := splitInfo.LastPart()
	if !ok {
		common.ExitOnErr(cmd, "", errors.New("missing any data in received object split information"))
	}

	common.PrintVerbose(cmd, "Traverse the object split chain in reverse...", idMember)

	var res *internal.HeadObjectRes
	chain := []oid.ID{idMember}
	chainSet := map[oid.ID]struct{}{idMember: {}}

	prmHead.SetRawFlag(false)
	// split members are almost definitely singular, but don't get hung up on it

	for {
		common.PrintVerbose(cmd, "Reading previous element of the split chain member %s...", idMember)

		addrObj.SetObject(idMember)

		res, err = internal.HeadObject(prmHead)
		common.ExitOnErr(cmd, "failed to read split chain member's header: %w", err)

		idMember, ok = res.Header().PreviousID()
		if !ok {
			common.PrintVerbose(cmd, "Chain ended.")
			break
		}

		if _, ok = chainSet[idMember]; ok {
			common.ExitOnErr(cmd, "", fmt.Errorf("duplicated member in the split chain %s", idMember))
		}

		chain = append(chain, idMember)
		chainSet[idMember] = struct{}{}
	}

	common.PrintVerbose(cmd, "Looking for a linking object...")

	var query object.SearchFilters
	query.AddParentIDFilter(object.MatchStringEqual, obj)

	var prmSearch internal.SearchObjectsPrm
	prmSearch.SetClient(cli)
	prmSearch.SetContainerID(cnr)
	prmSearch.SetFilters(query)

	resSearch, err := internal.SearchObjects(prmSearch)
	common.ExitOnErr(cmd, "failed to find object children: %w", err)

	list := resSearch.IDList()

	for i := range list {
		if _, ok = chainSet[list[i]]; !ok {
			common.PrintVerbose(cmd, "Found one more related object %s.", list[i])
			chain = append(chain, list[i])
		}
	}

	return chain
}
