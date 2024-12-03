package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	chaincontainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type MaxSizeSource interface {
	// MaxObjectSize returns maximum payload size
	// of physically stored object in system.
	//
	// Must return 0 if value can not be obtained.
	MaxObjectSize() uint64
}

type Service struct {
	*cfg
	transport Transport
	neoFSNet  NeoFSNetwork
}

type Option func(*cfg)

// Transport provides message transmission over NeoFS network.
type Transport interface {
	// SendReplicationRequestToNode sends a prepared replication request message to
	// the specified remote node.
	SendReplicationRequestToNode(ctx context.Context, req []byte, node client.NodeInfo) (*neofscrypto.Signature, error)
}

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

// ContainerNodes provides access to storage nodes matching storage policy of
// the particular container.
type ContainerNodes interface {
	// Unsorted returns unsorted descriptor set corresponding to the storage nodes
	// matching storage policy of the container. Nodes are identified by their
	// public keys and can be repeated in different sets.
	//
	// Unsorted callers do not change resulting slices and their elements.
	Unsorted() [][]netmapsdk.NodeInfo
	// SortForObject sorts container nodes for the referenced object's storage.
	//
	// SortForObject callers do not change resulting slices and their elements.
	SortForObject(oid.ID) ([][]netmapsdk.NodeInfo, error)
	// PrimaryCounts returns number (N) of primary object holders for each sorted
	// list (L) so:
	//  - size of each L >= N;
	//  - first N nodes of each L are primary data holders while others (if any)
	//    are backup.
	PrimaryCounts() []uint
}

// NeoFSNetwork provides access to the NeoFS network to get information
// necessary for the [Service] to work.
type NeoFSNetwork interface {
	// GetContainerNodes selects storage nodes matching storage policy of the
	// referenced container for now and provides [ContainerNodes] interface.
	//
	// Returns [apistatus.ContainerNotFound] if requested container is missing in
	// the network.
	GetContainerNodes(cid.ID) (ContainerNodes, error)
	// IsLocalNodePublicKey checks whether given binary-encoded public key is
	// assigned in the network map to a local storage node providing [Service].
	IsLocalNodePublicKey([]byte) bool
}

type cfg struct {
	keyStorage *objutil.KeyStorage

	maxSizeSrc MaxSizeSource

	localStore ObjectStorage

	cnrSrc container.Source

	netMapSrc netmap.Source

	remotePool, localPool util.WorkerPool

	fmtValidator *object.FormatValidator

	fmtValidatorOpts []object.FormatValidatorOption

	networkState netmap.State

	clientConstructor ClientConstructor

	log *zap.Logger

	networkMagic uint32

	cnrClient *chaincontainer.Client
}

func defaultCfg() *cfg {
	return &cfg{
		remotePool: util.NewPseudoWorkerPool(),
		localPool:  util.NewPseudoWorkerPool(),
		log:        zap.L(),
	}
}

func NewService(transport Transport, neoFSNet NeoFSNetwork, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.fmtValidator = object.NewFormatValidator(c.fmtValidatorOpts...)

	return &Service{
		cfg:       c,
		transport: transport,
		neoFSNet:  neoFSNet,
	}
}

func (p *Service) Put(ctx context.Context) (*Streamer, error) {
	return &Streamer{
		cfg:       p.cfg,
		ctx:       ctx,
		transport: p.transport,
		neoFSNet:  p.neoFSNet,
	}, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithMaxSizeSource(v MaxSizeSource) Option {
	return func(c *cfg) {
		c.maxSizeSrc = v
	}
}

func WithObjectStorage(v ObjectStorage) Option {
	return func(c *cfg) {
		c.localStore = v
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithLockSource(v))
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithNetworkMapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = v
	}
}

func WithWorkerPools(remote, local util.WorkerPool) Option {
	return func(c *cfg) {
		c.remotePool = remote
		c.localPool = local
	}
}

func WithNetworkState(v netmap.State) Option {
	return func(c *cfg) {
		c.networkState = v
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithNetState(v))
	}
}

func WithSplitChainVerifier(sv object.SplitVerifier) Option {
	return func(c *cfg) {
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithSplitVerifier(sv))
	}
}

func WithTombstoneVerifier(tv object.TombVerifier) Option {
	return func(c *cfg) {
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithTombVerifier(tv))
	}
}

func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientConstructor = v
	}
}

func WithContainerClient(v *chaincontainer.Client) Option {
	return func(c *cfg) {
		c.cnrClient = v
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

func WithNetworkMagic(m uint32) Option {
	return func(c *cfg) {
		c.networkMagic = m
	}
}
