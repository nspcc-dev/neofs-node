package putsvc

import (
	"context"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	chaincontainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// PaymentChecker defines interface that must keep container's payment status
// up-to-date.
type PaymentChecker interface {
	// IsPaid returns -1 epoch if container is paid and any non-negative epoch
	// if container was unpaid starting from that epoch.
	// It must return any error that does not allow ensure payment status.
	UnpaidSince(cID cid.ID) (unpaidFromEpoch int64, err error)
}

// QuotaLimiter describes limits for used space.
type QuotaLimiter interface {
	// AvailableQuotasLeft must return (soft limit, hard limit) pair for
	// provided container and user account. It limit is not set, [math.MaxUint64]
	// must be returned. Returned error should only mean there is not possibility
	// to fetch values from FS chain.
	AvailableQuotasLeft(cID cid.ID, owner user.ID) (uint64, uint64, error)
}

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
	SendReplicationRequestToNode(ctx context.Context, req []byte, node client.NodeInfo) ([]byte, error)
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
	// First PrimaryCounts() sets are for replication, the rest are for ECRules().
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
	// ECRules returns list of erasure coding rules for all objects in the
	// container. Same rule may repeat.
	//
	// ECRules callers do not change resulting slice.
	ECRules() []iec.Rule
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
	// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked.
	GetEpochBlock(epoch uint64) (uint32, error)
}

type cfg struct {
	keyStorage *objutil.KeyStorage

	maxSizeSrc MaxSizeSource

	localStore ObjectStorage

	cnrSrc container.Source

	remotePool util.WorkerPool

	fmtValidator *objectcore.FormatValidator

	fmtValidatorOpts []objectcore.FormatValidatorOption

	networkState netmap.StateDetailed

	clientConstructor ClientConstructor

	log *zap.Logger

	networkMagic uint32

	cnrClient *chaincontainer.Client

	metaSvc *meta.Meta

	quotaLimiter QuotaLimiter
	payments     PaymentChecker
}

func defaultCfg() *cfg {
	return &cfg{
		remotePool: util.NewPseudoWorkerPool(),
		log:        zap.L(),
	}
}

func NewService(transport Transport, neoFSNet NeoFSNetwork, m *meta.Meta, q QuotaLimiter, p PaymentChecker, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	var fmtValidatorChain objectcore.FSChain
	if c.cnrClient != nil {
		fmtValidatorChain = c.cnrClient.Morph()
	}

	c.fmtValidator = objectcore.NewFormatValidator(fmtValidatorChain, neoFSNet, c.cnrSrc, c.fmtValidatorOpts...)
	c.metaSvc = m
	c.quotaLimiter = q
	c.payments = p

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
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, objectcore.WithLockSource(v))
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithRemoteWorkerPool(remote util.WorkerPool) Option {
	return func(c *cfg) {
		c.remotePool = remote
	}
}

func WithNetworkState(v netmap.StateDetailed) Option {
	return func(c *cfg) {
		c.networkState = v
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, objectcore.WithNetState(v))
	}
}

func WithSplitChainVerifier(sv objectcore.SplitVerifier) Option {
	return func(c *cfg) {
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, objectcore.WithSplitVerifier(sv))
	}
}

func WithTombstoneVerifier(tv objectcore.TombVerifier) Option {
	return func(c *cfg) {
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, objectcore.WithTombVerifier(tv))
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
