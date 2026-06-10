package getsvc

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"io"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// NeoFSNetwork provides access to the NeoFS network to get information
// necessary for the [Service] to work.
type NeoFSNetwork interface {
	// GetNodesForObject returns descriptors of storage nodes matching storage
	// policy of the referenced object for now. Nodes are identified by their public
	// keys and can be repeated in different lists. First len(repRules) lists relate
	// to replication, the rest len(ecRules) - to EC.
	//
	// repRules specifies replication rules: the number (N) of primary object
	// holders for each list (L) so:
	//  - size of each L >= N;
	//  - first N nodes of each L are primary data holders while others (if any)
	//    are backup.
	//
	// ecRules specifies erasure coding rules for all objects in the container: each
	// object is split into [iec.Rule.DataPartNum] data and [iec.Rule.ParityPartNum]
	// parity parts. Each i-th part most expected to be located on SN described by
	// i-th list element. In general, list len is a multiple of CBF, and the part is
	// expected on N with index M*i, M in [0,CBF). Then part is expected on SN
	// for i+1-th part and so on.
	//
	// GetNodesForObject does not change resulting slices and their elements.
	//
	// Returns [apistatus.ContainerNotFound] if requested container is missing in
	// the network.
	GetNodesForObject(oid.Address) (nodeRules [][]netmap.NodeInfo, repRules []uint, ecRules []iec.Rule, err error)
	// IsLocalNodePublicKey checks whether given binary-encoded public key is
	// assigned in the network map to a local storage node providing [Service].
	IsLocalNodePublicKey([]byte) bool
}

// ErrResponded is returned when server successfully finished processing of some
// request.
var ErrResponded = errors.New("responded")

// ErrResponseStreamFailure is returned when response stream no longer works.
var ErrResponseStreamFailure = errors.New("stream failure")

// ErrAborted is returned to abort something.
var ErrAborted = errors.New("aborted")

// ErrLinker is returned when object of LINK type is received.
var ErrLinker = errors.New("linker")

// GetECRequestTransport is used to serve GET requests for EC objects.
type GetECRequestTransport interface {
	// CopyRemoteECPartParentHeaderAndPayload requests originally requested object's
	// EC part identified by partInfo from remote storage node using conn to it. If
	// succeeded, CopyRemoteECPartParentHeaderAndPayload sends parent header and
	// part payload to the client, and returns:
	//  - flag whether parent header was copied or not;
	//  - payload length of original requested object in bytes;
	//  - payload length of part object in bytes;
	//  - number of part's payload bytes copied.
	//
	// If response stream fails, [ErrResponseStreamFailure] is returned.
	//
	// If the node responds with split object info,
	// CopyRemoteECPartParentHeaderAndPayload converts it to
	// [*object.SplitInfoError] and returns.
	//
	// If the node responds with any failure status other than 'not found', the
	// response is copied to the client and [ErrResponded] is returned.
	//
	// If the node responds with object of LINK type, [ErrLinker] is returned.
	//
	// Otherwise, no error is returned. Copying can be incomplete in this case.
	//
	// CopyRemoteECPartParentHeaderAndPayload is never called concurrently.
	CopyRemoteECPartParentHeaderAndPayload(ctx context.Context, conn clientcore.MultiAddressClient, partInfo iec.PartInfo) (bool, uint64, uint64, uint64, error)
	// CopyLocalECPartParentHeaderAndPayload works like CopyRemoteECPartParentHeaderAndPayload but locally.
	CopyLocalECPartParentHeaderAndPayload(ctx context.Context, storage *engine.StorageEngine, partInfo iec.PartInfo) (bool, uint64, uint64, uint64, error)
	// CopyRemoteECPartRange requests specified payload range of originally
	// requested object's EC part identified by partInfo pair from remote storage
	// node using conn to it. If succeeded, CopyRemoteECPartRange sends with payload
	// range to the client, and returns number of bytes copied.
	//
	// If response stream fails, [ErrResponseStreamFailure] is returned.
	//
	// If the node responds with any failure status other than 'not found', the
	// response is copied to the client and [ErrResponded] is returned.
	//
	// Otherwise, no error is returned. Copying can be incomplete in this case.
	//
	// If controlCh is passed, CopyRemoteECPartRange blocks copying data until signal
	// from it. On true, CopyRemoteECPartRange returns [ErrAborted] instantly
	// without copying. Otherwise, i.e. if on false or close, copying starts.
	//
	// CopyRemoteECPartRange can be called concurrently for different partInfo, but
	// never for the same one.
	CopyRemoteECPartRange(ctx context.Context, conn clientcore.MultiAddressClient, partInfo iec.PartInfo, off, ln uint64, controlCh <-chan bool) (uint64, error)
	// CopyLocalECPartRange works like CopyRemoteECPartRange but locally.
	CopyLocalECPartRange(ctx context.Context, storage *engine.StorageEngine, partInfo iec.PartInfo, off, ln uint64, ch <-chan bool) (uint64, error)
}

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg
	neoFSNet NeoFSNetwork
}

// Option is a Service's constructor option.
type Option func(*cfg)

type getClient interface {
	getObject(*execCtx) (*object.Object, io.ReadCloser, error)
}

type cfg struct {
	log *zap.Logger

	// TODO: merge localStorage into localObjects
	localObjects interface {
		// GetECPart reads stored object that carries EC part produced within cnr for
		// parent object and indexed by pi.
		//
		// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
		// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
		GetECPart(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error)
		// GetECPartRange reads specified payload ranage of stored object that carries
		// EC part produced within cnr for parent object and indexed by pi.
		//
		// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
		// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
		// Returns [apistatus.ErrObjectNotFound] if the range is out of payload bounds.
		GetECPartRange(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) (uint64, io.ReadCloser, error)
		// ReadECPart is a buffered alternative for GetECPart similar to ReadObject.
		ReadECPart(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, io.ReadCloser, error)
		// ReadECPartRange is a buffered alternative for GetECPartRange similar to ReadECPart.
		ReadECPartRange(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64, buf []byte) (io.ReadCloser, error)
		Head(context.Context, oid.Address, bool) (*object.Object, error)
		ReadHeader(context.Context, oid.Address, bool, []byte) (int, error)
		// HeadECPart is similar to GetECPart but returns only the header.
		HeadECPart(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error)
		// ReadECPartHeader is a buffered alternative for HeadECPart similar to
		// ReadHeader.
		ReadECPartHeader(ctx context.Context, cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, error)
	}
	localStorage interface {
		get(*execCtx) (*object.Object, io.ReadCloser, error)
	}

	clientCache interface {
		get(context.Context, netmap.NodeInfo) (getClient, error)
	}
	// TODO: merge with clientCache
	// TODO: this differs with https://pkg.go.dev/github.com/nspcc-dev/neofs-sdk-go/client#Client
	//  interface because it cannot be fully overridden due to private fields. Consider exporting.
	conns interface {
		InitGetObjectStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			st *session.Object, local, verifyID bool, xs []string) (object.Object, io.ReadCloser, error)
		Head(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			st *session.Object) (object.Object, error)
		InitGetObjectRangeStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			off, ln uint64, st *session.Object, xs []string) (io.ReadCloser, error)
	}

	keyStore interface {
		GetKey(*user.ID) (*ecdsa.PrivateKey, error)
		GetKeyBySubjects([]sessionv2.Target) (*ecdsa.PrivateKey, error)
	}

	nnsResolver sessionv2.NNSResolver
}

func defaultCfg() *cfg {
	return &cfg{
		log:          zap.L(),
		localStorage: new(storageEngineWrapper),
		clientCache:  new(clientCacheWrapper),
	}
}

// New creates, initializes and returns utility serving
// Object.Get service requests.
func New(neoFSNet NeoFSNetwork, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg:      c,
		neoFSNet: neoFSNet,
	}
}

// WithLogger returns option to specify Get service's logger.
func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "Object.Get service"))
	}
}

// WithLocalStorageEngine returns option to set local storage
// instance.
func WithLocalStorageEngine(e *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localObjects = e
		c.localStorage.(*storageEngineWrapper).engine = e
	}
}

type ClientConstructor interface {
	Get(context.Context, netmap.NodeInfo) (clientcore.MultiAddressClient, error)
}

// WithClientConstructor returns option to set constructor of remote node clients.
func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).cache = v
		c.conns = c.clientCache.(*clientCacheWrapper)
	}
}

// WithKeyStorage returns option to set private
// key storage for session tokens and node key.
func WithKeyStorage(store *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStore = store
	}
}

// WithNNSResolver returns option to set NNS resolver for checking session token subjects.
func WithNNSResolver(resolver sessionv2.NNSResolver) Option {
	return func(c *cfg) {
		c.nnsResolver = resolver
	}
}

func (s *Service) logSNConnFailure(node netmap.NodeInfo, err error) {
	s.log.Warn("remote SN connection failure",
		zap.String("publicKey", hex.EncodeToString(node.PublicKey())),
		zap.Error(err),
	)
}
