package node

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/hash"
	apiobj "github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/session"
	eacl "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	object "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	storage2 "github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport/storagegroup"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	objectManagerParams struct {
		dig.In

		Logger     *zap.Logger
		Viper      *viper.Viper
		LocalStore localstore.Localstore

		PeersInterface peers.Interface

		Peers      peers.Store
		Placement  placement.Component
		TokenStore session.PrivateTokenStore
		Options    []string `name:"node_options"`
		Key        *ecdsa.PrivateKey

		NetMapClient *contract.Wrapper

		Placer *placement.PlacementWrapper

		ExtendedACLStore eacl.Storage

		ContainerStorage storage.Storage
	}
)

const (
	transformersSectionPath = "object.transformers."
)

const xorSalitor = "xor"

func newObjectManager(p objectManagerParams) (object.Service, error) {
	var sltr object.Salitor

	if p.Viper.GetString("object.salitor") == xorSalitor {
		sltr = hash.SaltXOR
	}

	as, err := storage2.NewAddressStore(p.Peers, p.Logger)
	if err != nil {
		return nil, err
	}

	rs := object.NewRemoteService(p.PeersInterface)

	pto := p.Viper.GetDuration("object.put.timeout")
	gto := p.Viper.GetDuration("object.get.timeout")
	hto := p.Viper.GetDuration("object.head.timeout")
	sto := p.Viper.GetDuration("object.search.timeout")
	rhto := p.Viper.GetDuration("object.range_hash.timeout")
	dto := p.Viper.GetDuration("object.dial_timeout")

	tr, err := object.NewMultiTransport(object.MultiTransportParams{
		AddressStore:     as,
		EpochReceiver:    p.Placer,
		RemoteService:    rs,
		Logger:           p.Logger,
		Key:              p.Key,
		PutTimeout:       pto,
		GetTimeout:       gto,
		HeadTimeout:      hto,
		SearchTimeout:    sto,
		RangeHashTimeout: rhto,
		DialTimeout:      dto,

		PrivateTokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	exec, err := transport.NewContainerTraverseExecutor(tr)
	if err != nil {
		return nil, err
	}

	selectiveExec, err := transport.NewObjectContainerHandler(transport.ObjectContainerHandlerParams{
		NodeLister: p.Placer,
		Executor:   exec,
		Logger:     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	sgInfoRecv, err := storagegroup.NewStorageGroupInfoReceiver(storagegroup.StorageGroupInfoReceiverParams{
		SelectiveContainerExecutor: selectiveExec,
		Logger:                     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	verifier, err := storage2.NewLocalIntegrityVerifier()
	if err != nil {
		return nil, err
	}

	trans, err := transformer.NewTransformer(transformer.Params{
		SGInfoReceiver: sgInfoRecv,
		EpochReceiver:  p.Placer,
		SizeLimit:      uint64(p.Viper.GetInt64(transformersSectionPath+"payload_limiter.max_payload_size") * apiobj.UnitsKB),
		Verifier:       verifier,
	})
	if err != nil {
		return nil, err
	}

	verifier, err = storage2.NewLocalHeadIntegrityVerifier()
	if err != nil {
		return nil, err
	}

	return object.New(&object.Params{
		Verifier:          verifier,
		Salitor:           sltr,
		LocalStore:        p.LocalStore,
		MaxProcessingSize: p.Viper.GetUint64("object.max_processing_size") * uint64(apiobj.UnitsMB),
		StorageCapacity:   bootstrap.NodeInfo{Options: p.Options}.Capacity() * uint64(apiobj.UnitsGB),
		PoolSize:          p.Viper.GetInt("object.workers_count"),
		Placer:            p.Placer,
		Transformer:       trans,
		ObjectRestorer: transformer.NewRestorePipeline(
			transformer.SplitRestorer(),
		),
		RemoteService:    rs,
		AddressStore:     as,
		Logger:           p.Logger,
		TokenStore:       p.TokenStore,
		EpochReceiver:    p.Placer,
		PlacementWrapper: p.Placer,
		Key:              p.Key,
		CheckACL:         p.Viper.GetBool("object.check_acl"),
		DialTimeout:      p.Viper.GetDuration("object.dial_timeout"),
		MaxPayloadSize:   p.Viper.GetUint64("object.transformers.payload_limiter.max_payload_size") * uint64(apiobj.UnitsKB),
		PutParams: object.OperationParams{
			Timeout:   pto,
			LogErrors: p.Viper.GetBool("object.put.log_errs"),
		},
		GetParams: object.OperationParams{
			Timeout:   gto,
			LogErrors: p.Viper.GetBool("object.get.log_errs"),
		},
		HeadParams: object.OperationParams{
			Timeout:   hto,
			LogErrors: p.Viper.GetBool("object.head.log_errs"),
		},
		DeleteParams: object.OperationParams{
			Timeout:   p.Viper.GetDuration("object.delete.timeout"),
			LogErrors: p.Viper.GetBool("object.get.log_errs"),
		},
		SearchParams: object.OperationParams{
			Timeout:   sto,
			LogErrors: p.Viper.GetBool("object.search.log_errs"),
		},
		RangeParams: object.OperationParams{
			Timeout:   p.Viper.GetDuration("object.range.timeout"),
			LogErrors: p.Viper.GetBool("object.range.log_errs"),
		},
		RangeHashParams: object.OperationParams{
			Timeout:   rhto,
			LogErrors: p.Viper.GetBool("object.range_hash.log_errs"),
		},
		Assembly: p.Viper.GetBool("object.assembly"),

		WindowSize: p.Viper.GetInt("object.window_size"),

		ContainerStorage: p.ContainerStorage,
		NetmapClient:     p.NetMapClient,

		SGInfoReceiver: sgInfoRecv,

		ExtendedACLSource: p.ExtendedACLStore,
	})
}
