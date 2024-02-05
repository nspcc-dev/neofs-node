package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

	node Node
}

type Option func(*cfg)

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

// ObjectStoragePolicy contains rules for storing objects in the NeoFS network.
type ObjectStoragePolicy interface {
	// StorageNodesForObject applies rules related to the referenced object and
	// returns ordered lists of nodes to store the object: first N (second return)
	// nodes in each list are primary object holders while others (if any) are
	// backup.
	StorageNodesForObject(oid.ID) ([][]netmapsdk.NodeInfo, []uint32, error)
}

// Node represents local NeoFS storage node within which [Service] operates.
type Node interface {
	// ObjectStoragePolicyForContainer returns [ObjectStoragePolicy] for objects
	// bound to the referenced container.
	//
	// Returns [apistatus.ContainerNotFound] if specified container is missing in
	// the network.
	ObjectStoragePolicyForContainer(cid.ID) (ObjectStoragePolicy, error)

	// IsLocalPublicKey checks whether given binary-encoded public key is announced
	// by the Node in the NeoFS network map.
	IsLocalPublicKey(bPubKey []byte) bool
}

type cfg struct {
	keyStorage *objutil.KeyStorage

	maxSizeSrc MaxSizeSource

	localStore ObjectStorage

	cnrSrc container.Source

	remotePool, localPool util.WorkerPool

	fmtValidator *object.FormatValidator

	fmtValidatorOpts []object.FormatValidatorOption

	networkState netmap.State

	clientConstructor ClientConstructor

	log *zap.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		remotePool: util.NewPseudoWorkerPool(),
		localPool:  util.NewPseudoWorkerPool(),
		log:        zap.L(),
	}
}

func NewService(node Node, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.fmtValidator = object.NewFormatValidator(c.fmtValidatorOpts...)

	return &Service{
		cfg:  c,
		node: node,
	}
}

func (p *Service) Put(ctx context.Context) (*Streamer, error) {
	return &Streamer{
		Service: p,
		ctx:     ctx,
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

func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientConstructor = v
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}
