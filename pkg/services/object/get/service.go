package getsvc

import (
	"context"
	"crypto/ecdsa"
	"io"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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
	GetNodesForObject(oid.Address) (nodeRules [][]netmapsdk.NodeInfo, repRules []uint, ecRules []iec.Rule, err error)
	// IsLocalNodePublicKey checks whether given binary-encoded public key is
	// assigned in the network map to a local storage node providing [Service].
	IsLocalNodePublicKey([]byte) bool
}

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg
	neoFSNet NeoFSNetwork
}

// Option is a Service's constructor option.
type Option func(*cfg)

type getClient interface {
	getObject(*execCtx, client.NodeInfo) (*object.Object, io.ReadCloser, error)
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
		GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error)
		// GetECPartRange reads specified payload ranage of stored object that carries
		// EC part produced within cnr for parent object and indexed by pi.
		//
		// Returns [apistatus.ErrObjectAlreadyRemoved] if the object was marked for
		// removal. Returns [apistatus.ErrObjectNotFound] if the object is missing.
		// Returns [apistatus.ErrObjectNotFound] if the range is out of payload bounds.
		GetECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) (uint64, io.ReadCloser, error)
		Head(oid.Address, bool) (*object.Object, error)
		// HeadECPart is similar to GetECPart but returns only the header.
		HeadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error)
		HeadToBuffer(oid.Address, bool, func() []byte) (int, error)
		OpenStream(oid.Address, func() []byte) (int, io.ReadCloser, error)
	}
	localStorage interface {
		get(*execCtx) (*object.Object, io.ReadCloser, error)
	}

	clientCache interface {
		get(client.NodeInfo) (getClient, error)
	}
	// TODO: merge with clientCache
	// TODO: this differs with https://pkg.go.dev/github.com/nspcc-dev/neofs-sdk-go/client#Client
	//  interface because it cannot be fully overridden due to private fields. Consider exporting.
	conns interface {
		InitGetObjectStream(ctx context.Context, node netmapsdk.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			st *session.Object, local, verifyID bool, xs []string) (object.Object, io.ReadCloser, error)
		Head(ctx context.Context, node netmapsdk.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			st *session.Object) (object.Object, error)
		InitGetObjectRangeStream(ctx context.Context, node netmapsdk.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
			off, ln uint64, st *session.Object, xs []string) (io.ReadCloser, error)
	}

	keyStore interface {
		GetKey(*util.SessionInfo) (*ecdsa.PrivateKey, error)
	}
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
	Get(client.NodeInfo) (client.MultiAddressClient, error)
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
