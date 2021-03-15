package main

import (
	"context"

	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	deletesvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/delete/v2"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	getsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/get/v2"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	putsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/put/v2"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	searchsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/search/v2"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/gc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type objectSvc struct {
	put *putsvcV2.Service

	search *searchsvcV2.Service

	get *getsvcV2.Service

	delete *deletesvcV2.Service
}

func (c *cfg) MaxObjectSize() uint64 {
	sz, err := c.cfgNetmap.wrapper.MaxObjectSize()
	if err != nil {
		c.log.Error("could not get max object size value",
			zap.String("error", err.Error()),
		)
	}

	return sz
}

func (s *objectSvc) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	return s.put.Put(ctx)
}

func (s *objectSvc) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	return s.get.Head(ctx, req)
}

func (s *objectSvc) Search(req *object.SearchRequest, stream objectService.SearchStream) error {
	return s.search.Search(req, stream)
}

func (s *objectSvc) Get(req *object.GetRequest, stream objectService.GetObjectStream) error {
	return s.get.Get(req, stream)
}

func (s *objectSvc) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	return s.delete.Delete(ctx, req)
}

func (s *objectSvc) GetRange(req *object.GetRangeRequest, stream objectService.GetObjectRangeStream) error {
	return s.get.GetRange(req, stream)
}

func (s *objectSvc) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return s.get.GetRangeHash(ctx, req)
}

type localObjectRemover struct {
	storage *engine.StorageEngine

	log *logger.Logger
}

type localObjectInhumer struct {
	storage *engine.StorageEngine

	log *logger.Logger
}

func (r *localObjectRemover) Delete(addr ...*objectSDK.Address) error {
	_, err := r.storage.Delete(new(engine.DeletePrm).
		WithAddresses(addr...),
	)

	return err
}

func (r *localObjectInhumer) DeleteObjects(ts *objectSDK.Address, addr ...*objectSDK.Address) {
	prm := new(engine.InhumePrm)

	for _, a := range addr {
		prm.WithTarget(ts, a)

		if _, err := r.storage.Inhume(prm); err != nil {
			r.log.Error("could not delete object",
				zap.Stringer("address", a),
				zap.String("error", err.Error()),
			)
		}
	}
}

type delNetInfo struct {
	netmap.State

	tsLifetime uint64
}

func (i *delNetInfo) TombstoneLifetime() (uint64, error) {
	return i.tsLifetime, nil
}

func initObjectService(c *cfg) {
	ls := c.cfgObject.cfgLocalStorage.localStorage
	keyStorage := util.NewKeyStorage(c.key, c.privateTokenStore)
	nodeOwner := owner.NewID()

	neo3Wallet, err := owner.NEO3WalletFromPublicKey(&c.key.PublicKey)
	fatalOnErr(err)

	nodeOwner.SetNeo3Wallet(neo3Wallet)

	clientCache := cache.NewSDKClientCache(
		client.WithDialTimeout(c.viper.GetDuration(cfgDialTimeout)))

	objRemover := &localObjectRemover{
		storage: ls,
		log:     c.log,
	}

	objInhumer := &localObjectInhumer{
		storage: ls,
		log:     c.log,
	}

	objGC := gc.New(
		gc.WithLogger(c.log),
		gc.WithRemover(objRemover),
		gc.WithQueueCapacity(c.viper.GetUint32(cfgGCQueueSize)),
		gc.WithSleepInterval(c.viper.GetDuration(cfgGCQueueTick)),
		gc.WithWorkingInterval(c.viper.GetDuration(cfgGCTimeout)),
	)

	c.workers = append(c.workers, objGC)

	repl := replicator.New(
		replicator.WithLogger(c.log),
		replicator.WithPutTimeout(
			c.viper.GetDuration(cfgReplicatorPutTimeout),
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, clientCache),
		),
	)

	c.workers = append(c.workers, repl)

	ch := make(chan *policer.Task, 1)

	pol := policer.New(
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithContainerSource(c.cfgObject.cnrStorage),
		policer.WithPlacementBuilder(
			placement.NewNetworkMapSourceBuilder(c.cfgObject.netMapStorage),
		),
		policer.WithWorkScope(
			c.viper.GetInt(cfgPolicerWorkScope),
		),
		policer.WithExpansionRate(
			c.viper.GetInt(cfgPolicerExpRate),
		),
		policer.WithTrigger(ch),
		policer.WithRemoteHeader(
			headsvc.NewRemoteHeader(keyStorage, clientCache),
		),
		policer.WithLocalAddressSource(c),
		policer.WithHeadTimeout(
			c.viper.GetDuration(cfgPolicerHeadTimeout),
		),
		policer.WithReplicator(repl),
		policer.WithRedundantCopyCallback(func(addr *objectSDK.Address) {
			_, err := ls.Inhume(new(engine.InhumePrm).MarkAsGarbage(addr))
			if err != nil {
				c.log.Warn("could not inhume mark redundant copy as garbage",
					zap.String("error", err.Error()),
				)
			}
		}),
	)

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		select {
		case ch <- new(policer.Task):
		case <-c.ctx.Done():
			close(ch)
		default:
			c.log.Info("policer is busy")
		}
	})

	traverseGen := util.NewTraverserGenerator(c.cfgObject.netMapStorage, c.cfgObject.cnrStorage, c)

	c.workers = append(c.workers, pol)

	sPut := putsvc.NewService(
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientCache(clientCache),
		putsvc.WithMaxSizeSource(c),
		putsvc.WithLocalStorage(ls),
		putsvc.WithContainerSource(c.cfgObject.cnrStorage),
		putsvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		putsvc.WithLocalAddressSource(c),
		putsvc.WithFormatValidatorOpts(
			objectCore.WithDeleteHandler(objInhumer),
		),
		putsvc.WithNetworkState(c.cfgNetmap.state),
		putsvc.WithWorkerPool(c.cfgObject.pool.put),
		putsvc.WithLogger(c.log),
	)

	sPutV2 := putsvcV2.NewService(
		putsvcV2.WithInternalService(sPut),
	)

	sSearch := searchsvc.New(
		searchsvc.WithLogger(c.log),
		searchsvc.WithLocalStorageEngine(ls),
		searchsvc.WithClientCache(clientCache),
		searchsvc.WithTraverserGenerator(
			traverseGen.WithTraverseOptions(
				placement.WithoutSuccessTracking(),
			),
		),
		searchsvc.WithNetMapSource(c.cfgNetmap.wrapper),
	)

	sSearchV2 := searchsvcV2.NewService(
		searchsvcV2.WithInternalService(sSearch),
		searchsvcV2.WithKeyStorage(keyStorage),
	)

	sGet := getsvc.New(
		getsvc.WithLogger(c.log),
		getsvc.WithLocalStorageEngine(ls),
		getsvc.WithClientCache(clientCache),
		getsvc.WithTraverserGenerator(
			traverseGen.WithTraverseOptions(
				placement.SuccessAfter(1),
			),
		),
		getsvc.WithNetMapSource(c.cfgNetmap.wrapper),
	)

	sGetV2 := getsvcV2.NewService(
		getsvcV2.WithInternalService(sGet),
		getsvcV2.WithKeyStorage(keyStorage),
	)

	sDelete := deletesvc.New(
		deletesvc.WithLogger(c.log),
		deletesvc.WithHeadService(sGet),
		deletesvc.WithSearchService(sSearch),
		deletesvc.WithPutService(sPut),
		deletesvc.WithNetworkInfo(&delNetInfo{
			State:      c.cfgNetmap.state,
			tsLifetime: c.viper.GetUint64(cfgTombstoneLifetime),
		}),
	)

	sDeleteV2 := deletesvcV2.NewService(
		deletesvcV2.WithInternalService(sDelete),
		deletesvcV2.WithKeyStorage(keyStorage),
	)

	// build service pipeline
	// grpc | acl | signature | response | split

	splitSvc := objectService.NewTransportSplitter(
		c.cfgGRPC.maxChunkSize,
		c.cfgGRPC.maxAddrAmount,
		&objectSvc{
			put:    sPutV2,
			search: sSearchV2,
			get:    sGetV2,
			delete: sDeleteV2,
		},
	)

	respSvc := objectService.NewResponseService(
		splitSvc,
		c.respSvc,
	)

	signSvc := objectService.NewSignService(
		c.key,
		respSvc,
	)

	aclSvc := acl.New(
		acl.WithSenderClassifier(
			acl.NewSenderClassifier(
				c.log,
				c.cfgNetmap.wrapper,
				c.cfgNetmap.wrapper,
			),
		),
		acl.WithContainerSource(
			c.cfgObject.cnrStorage,
		),
		acl.WithNextService(signSvc),
		acl.WithLocalStorage(ls),
		acl.WithEACLValidatorOptions(
			eacl.WithEACLStorage(newCachedEACLStorage(&morphEACLStorage{
				w: c.cfgObject.cnrClient,
			})),
			eacl.WithLogger(c.log),
		),
		acl.WithNetmapState(c.cfgNetmap.state),
	)

	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(aclSvc),
	)
}

type morphEACLStorage struct {
	w *wrapper.Wrapper
}

type signedEACLTable eaclSDK.Table

func (s *signedEACLTable) ReadSignedData(buf []byte) ([]byte, error) {
	return (*eaclSDK.Table)(s).Marshal(buf)
}

func (s *signedEACLTable) SignedDataSize() int {
	// TODO: add eacl.Table.Size method
	return (*eaclSDK.Table)(s).ToV2().StableSize()
}

func (s *morphEACLStorage) GetEACL(cid *container.ID) (*eaclSDK.Table, error) {
	table, sig, err := s.w.GetEACL(cid)
	if err != nil {
		return nil, err
	}

	if err := signature.VerifyDataWithSource(
		(*signedEACLTable)(table),
		func() ([]byte, []byte) {
			return sig.Key(), sig.Sign()
		},
		signature.SignWithRFC6979(),
	); err != nil {
		return nil, errors.Wrap(err, "incorrect signature")
	}

	return table, nil
}
