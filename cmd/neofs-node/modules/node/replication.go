package node

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/morph"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	replicationManagerParams struct {
		dig.In

		Viper *viper.Viper

		PeersInterface peers.Interface

		LocalStore localstore.Localstore
		Peers      peers.Store
		Placement  placement.Component
		Logger     *zap.Logger
		Key        *ecdsa.PrivateKey

		Placer *placement.PlacementWrapper

		TokenStore session.PrivateTokenStore

		MorphEventListener event.Listener
		MorphEventHandlers morph.EventHandlers
	}
)

const (
	mainReplicationPrefix  = "replication"
	managerPrefix          = "manager"
	placementHonorerPrefix = "placement_honorer"
	locationDetectorPrefix = "location_detector"
	storageValidatorPrefix = "storage_validator"
	replicatorPrefix       = "replicator"
	restorerPrefix         = "restorer"
)

func newReplicationManager(p replicationManagerParams) (replication.Manager, error) {
	as, err := storage.NewAddressStore(p.Peers, p.Logger)
	if err != nil {
		return nil, err
	}

	ms, err := replication.NewMultiSolver(replication.MultiSolverParams{
		AddressStore: as,
		Placement:    p.Placement,
	})
	if err != nil {
		return nil, err
	}

	op := replication.NewObjectPool()

	schd, err := replication.NewReplicationScheduler(replication.SchedulerParams{
		ContainerActualityChecker: ms,
		Iterator:                  p.LocalStore,
	})
	if err != nil {
		return nil, err
	}

	integrityVerifier, err := storage.NewLocalIntegrityVerifier()
	if err != nil {
		return nil, err
	}

	verifier, err := storage.NewObjectValidator(&storage.ObjectValidatorParams{
		AddressStore: ms,
		Localstore:   p.LocalStore,
		Logger:       p.Logger,
		Verifier:     integrityVerifier,
	})
	if err != nil {
		return nil, err
	}

	placementHonorer, err := newPlacementHonorer(p, ms)
	if err != nil {
		return nil, err
	}

	locationDetector, err := newLocationDetector(p, ms)
	if err != nil {
		return nil, err
	}

	storageValidator, err := newStorageValidator(p, ms)
	if err != nil {
		return nil, err
	}

	replicator, err := newObjectReplicator(p, ms)
	if err != nil {
		return nil, err
	}

	restorer, err := newRestorer(p, ms)
	if err != nil {
		return nil, err
	}

	prefix := mainReplicationPrefix + "." + managerPrefix + "."
	capPrefix := prefix + "capacities."

	mngr, err := replication.NewManager(replication.ManagerParams{
		Interval:                p.Viper.GetDuration(prefix + "read_pool_interval"),
		PushTaskTimeout:         p.Viper.GetDuration(prefix + "push_task_timeout"),
		InitPoolSize:            p.Viper.GetInt(prefix + "pool_size"),
		ExpansionRate:           p.Viper.GetFloat64(prefix + "pool_expansion_rate"),
		PlacementHonorerEnabled: p.Viper.GetBool(prefix + "placement_honorer_enabled"),
		ReplicateTaskChanCap:    p.Viper.GetInt(capPrefix + "replicate"),
		RestoreTaskChanCap:      p.Viper.GetInt(capPrefix + "restore"),
		GarbageChanCap:          p.Viper.GetInt(capPrefix + "garbage"),
		ObjectPool:              op,
		ObjectVerifier:          verifier,
		PlacementHonorer:        placementHonorer,
		ObjectLocationDetector:  locationDetector,
		StorageValidator:        storageValidator,
		ObjectReplicator:        replicator,
		ObjectRestorer:          restorer,
		Scheduler:               schd,
		Logger:                  p.Logger,
	})
	if err != nil {
		return nil, err
	}

	if handlerInfo, ok := p.MorphEventHandlers[morph.ContractEventOptPath(
		morph.NetmapContractName,
		morph.NewEpochEventType,
	)]; ok {
		handlerInfo.SetHandler(func(ev event.Event) {
			mngr.HandleEpoch(
				context.Background(),
				ev.(netmap.NewEpoch).EpochNumber(),
			)
		})

		p.MorphEventListener.RegisterHandler(handlerInfo)
	}

	return mngr, nil
}

func newPlacementHonorer(p replicationManagerParams, rss replication.RemoteStorageSelector) (replication.PlacementHonorer, error) {
	prefix := mainReplicationPrefix + "." + placementHonorerPrefix

	och, err := newObjectsContainerHandler(cnrHandlerParams{
		Viper:          p.Viper,
		Logger:         p.Logger,
		Placer:         p.Placer,
		PeerStore:      p.Peers,
		Peers:          p.PeersInterface,
		TimeoutsPrefix: prefix,
		Key:            p.Key,

		TokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewObjectStorage(storage.ObjectStorageParams{
		Localstore:                 p.LocalStore,
		SelectiveContainerExecutor: och,
		Logger:                     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return replication.NewPlacementHonorer(replication.PlacementHonorerParams{
		ObjectSource:          storage,
		ObjectReceptacle:      storage,
		RemoteStorageSelector: rss,
		PresenceChecker:       p.LocalStore,
		Logger:                p.Logger,
		TaskChanCap:           p.Viper.GetInt(prefix + ".chan_capacity"),
		ResultTimeout:         p.Viper.GetDuration(prefix + ".result_timeout"),
	})
}

func newLocationDetector(p replicationManagerParams, ms replication.MultiSolver) (replication.ObjectLocationDetector, error) {
	prefix := mainReplicationPrefix + "." + locationDetectorPrefix

	och, err := newObjectsContainerHandler(cnrHandlerParams{
		Viper:          p.Viper,
		Logger:         p.Logger,
		Placer:         p.Placer,
		PeerStore:      p.Peers,
		Peers:          p.PeersInterface,
		TimeoutsPrefix: prefix,
		Key:            p.Key,

		TokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	locator, err := storage.NewObjectLocator(storage.LocatorParams{
		SelectiveContainerExecutor: och,
		Logger:                     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return replication.NewLocationDetector(&replication.LocationDetectorParams{
		WeightComparator:         ms,
		ObjectLocator:            locator,
		ReservationRatioReceiver: ms,
		PresenceChecker:          p.LocalStore,
		Logger:                   p.Logger,
		TaskChanCap:              p.Viper.GetInt(prefix + ".chan_capacity"),
		ResultTimeout:            p.Viper.GetDuration(prefix + ".result_timeout"),
	})
}

func newStorageValidator(p replicationManagerParams, as replication.AddressStore) (replication.StorageValidator, error) {
	prefix := mainReplicationPrefix + "." + storageValidatorPrefix

	var sltr storage.Salitor

	switch v := p.Viper.GetString(prefix + ".salitor"); v {
	case xorSalitor:
		sltr = hash.SaltXOR
	default:
		return nil, errors.Errorf("unsupported salitor: %s", v)
	}

	och, err := newObjectsContainerHandler(cnrHandlerParams{
		Viper:          p.Viper,
		Logger:         p.Logger,
		Placer:         p.Placer,
		PeerStore:      p.Peers,
		Peers:          p.PeersInterface,
		TimeoutsPrefix: prefix,
		Key:            p.Key,

		TokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	headVerifier, err := storage.NewLocalHeadIntegrityVerifier()
	if err != nil {
		return nil, err
	}

	verifier, err := storage.NewObjectValidator(&storage.ObjectValidatorParams{
		AddressStore:               as,
		Localstore:                 p.LocalStore,
		SelectiveContainerExecutor: och,
		Logger:                     p.Logger,
		Salitor:                    sltr,
		SaltSize:                   p.Viper.GetInt(prefix + ".salt_size"),
		MaxPayloadRangeSize:        p.Viper.GetUint64(prefix + ".max_payload_range_size"),
		PayloadRangeCount:          p.Viper.GetInt(prefix + ".payload_range_count"),
		Verifier:                   headVerifier,
	})
	if err != nil {
		return nil, err
	}

	return replication.NewStorageValidator(replication.StorageValidatorParams{
		ObjectVerifier:  verifier,
		PresenceChecker: p.LocalStore,
		Logger:          p.Logger,
		TaskChanCap:     p.Viper.GetInt(prefix + ".chan_capacity"),
		ResultTimeout:   p.Viper.GetDuration(prefix + ".result_timeout"),
		AddrStore:       as,
	})
}

func newObjectReplicator(p replicationManagerParams, rss replication.RemoteStorageSelector) (replication.ObjectReplicator, error) {
	prefix := mainReplicationPrefix + "." + replicatorPrefix

	och, err := newObjectsContainerHandler(cnrHandlerParams{
		Viper:          p.Viper,
		Logger:         p.Logger,
		Placer:         p.Placer,
		PeerStore:      p.Peers,
		Peers:          p.PeersInterface,
		TimeoutsPrefix: prefix,
		Key:            p.Key,

		TokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewObjectStorage(storage.ObjectStorageParams{
		Localstore:                 p.LocalStore,
		SelectiveContainerExecutor: och,
		Logger:                     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return replication.NewReplicator(replication.ObjectReplicatorParams{
		RemoteStorageSelector: rss,
		ObjectSource:          storage,
		ObjectReceptacle:      storage,
		PresenceChecker:       p.LocalStore,
		Logger:                p.Logger,
		TaskChanCap:           p.Viper.GetInt(prefix + ".chan_capacity"),
		ResultTimeout:         p.Viper.GetDuration(prefix + ".result_timeout"),
	})
}

func newRestorer(p replicationManagerParams, ms replication.MultiSolver) (replication.ObjectRestorer, error) {
	prefix := mainReplicationPrefix + "." + restorerPrefix

	och, err := newObjectsContainerHandler(cnrHandlerParams{
		Viper:          p.Viper,
		Logger:         p.Logger,
		Placer:         p.Placer,
		PeerStore:      p.Peers,
		Peers:          p.PeersInterface,
		TimeoutsPrefix: prefix,
		Key:            p.Key,

		TokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	integrityVerifier, err := storage.NewLocalIntegrityVerifier()
	if err != nil {
		return nil, err
	}

	verifier, err := storage.NewObjectValidator(&storage.ObjectValidatorParams{
		AddressStore:               ms,
		Localstore:                 p.LocalStore,
		SelectiveContainerExecutor: och,
		Logger:                     p.Logger,
		Verifier:                   integrityVerifier,
	})
	if err != nil {
		return nil, err
	}

	storage, err := storage.NewObjectStorage(storage.ObjectStorageParams{
		Localstore: p.LocalStore,
		Logger:     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return replication.NewObjectRestorer(&replication.ObjectRestorerParams{
		ObjectVerifier:        verifier,
		ObjectReceptacle:      storage,
		EpochReceiver:         ms,
		RemoteStorageSelector: ms,
		PresenceChecker:       p.LocalStore,
		Logger:                p.Logger,
		TaskChanCap:           p.Viper.GetInt(prefix + ".chan_capacity"),
		ResultTimeout:         p.Viper.GetDuration(prefix + ".result_timeout"),
	})
}
