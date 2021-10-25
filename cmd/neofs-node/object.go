package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	policerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/policer"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cntrwrp "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmwrp "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
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

func (s *objectSvc) Put(ctx context.Context) (objectService.PutObjectStream, error) {
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

type localObjectInhumer struct {
	storage *engine.StorageEngine

	log *logger.Logger
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

type innerRingFetcherWithNotary struct {
	sidechain *morphClient.Client
}

func (fn *innerRingFetcherWithNotary) InnerRingKeys() ([][]byte, error) {
	keys, err := fn.sidechain.NeoFSAlphabetList()
	if err != nil {
		return nil, fmt.Errorf("can't get inner ring keys from alphabet role: %w", err)
	}

	result := make([][]byte, 0, len(keys))
	for i := range keys {
		result = append(result, keys[i].Bytes())
	}

	return result, nil
}

type innerRingFetcherWithoutNotary struct {
	nm *nmwrp.Wrapper
}

func (f *innerRingFetcherWithoutNotary) InnerRingKeys() ([][]byte, error) {
	keys, err := f.nm.GetInnerRingList()
	if err != nil {
		return nil, fmt.Errorf("can't get inner ring keys from netmap contract: %w", err)
	}

	result := make([][]byte, 0, len(keys))
	for i := range keys {
		result = append(result, keys[i].Bytes())
	}

	return result, nil
}

type coreClientConstructor reputationClientConstructor

func (x *coreClientConstructor) Get(info coreclient.NodeInfo) (coreclient.Client, error) {
	c, err := (*reputationClientConstructor)(x).Get(info)
	if err != nil {
		return nil, err
	}

	return c.(coreclient.Client), nil
}

func initObjectService(c *cfg) {
	ls := c.cfgObject.cfgLocalStorage.localStorage
	keyStorage := util.NewKeyStorage(&c.key.PrivateKey, c.privateTokenStore, c.cfgNetmap.state)
	nodeOwner := owner.NewID()

	neo3Wallet, err := owner.NEO3WalletFromPublicKey((*ecdsa.PublicKey)(c.key.PublicKey()))
	fatalOnErr(err)

	nodeOwner.SetNeo3Wallet(neo3Wallet)

	clientConstructor := &reputationClientConstructor{
		log:              c.log,
		nmSrc:            c.cfgObject.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.clientCache,
	}

	coreConstructor := (*coreClientConstructor)(clientConstructor)

	var irFetcher acl.InnerRingFetcher

	if c.cfgMorph.client.ProbeNotary() {
		irFetcher = &innerRingFetcherWithNotary{
			sidechain: c.cfgMorph.client,
		}
	} else {
		irFetcher = &innerRingFetcherWithoutNotary{
			nm: c.cfgNetmap.wrapper,
		}
	}

	objInhumer := &localObjectInhumer{
		storage: ls,
		log:     c.log,
	}

	repl := replicator.New(
		replicator.WithLogger(c.log),
		replicator.WithPutTimeout(
			replicatorconfig.PutTimeout(c.appCfg),
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, coreConstructor),
		),
	)

	c.workers = append(c.workers, repl)

	ch := make(chan *policer.Task, 1)

	pol := policer.New(
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithContainerSource(c.cfgObject.cnrSource),
		policer.WithPlacementBuilder(
			placement.NewNetworkMapSourceBuilder(c.cfgObject.netMapSource),
		),
		policer.WithWorkScope(100),
		policer.WithExpansionRate(10),
		policer.WithTrigger(ch),
		policer.WithRemoteHeader(
			headsvc.NewRemoteHeader(keyStorage, clientConstructor),
		),
		policer.WithNetmapKeys(c),
		policer.WithHeadTimeout(
			policerconfig.HeadTimeout(c.appCfg),
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

	traverseGen := util.NewTraverserGenerator(c.cfgObject.netMapSource, c.cfgObject.cnrSource, c)

	c.workers = append(c.workers, pol)

	sPut := putsvc.NewService(
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(coreConstructor),
		putsvc.WithMaxSizeSource(c),
		putsvc.WithLocalStorage(ls),
		putsvc.WithContainerSource(c.cfgObject.cnrSource),
		putsvc.WithNetworkMapSource(c.cfgObject.netMapSource),
		putsvc.WithNetmapKeys(c),
		putsvc.WithFormatValidatorOpts(
			objectCore.WithDeleteHandler(objInhumer),
		),
		putsvc.WithNetworkState(c.cfgNetmap.state),
		putsvc.WithWorkerPools(c.cfgObject.pool.putRemote),
		putsvc.WithLogger(c.log),
	)

	sPutV2 := putsvcV2.NewService(
		putsvcV2.WithInternalService(sPut),
		putsvcV2.WithKeyStorage(keyStorage),
	)

	sSearch := searchsvc.New(
		searchsvc.WithLogger(c.log),
		searchsvc.WithLocalStorageEngine(ls),
		searchsvc.WithClientConstructor(coreConstructor),
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
		getsvc.WithClientConstructor(coreConstructor),
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
			tsLifetime: 5,
		}),
	)

	sDeleteV2 := deletesvcV2.NewService(
		deletesvcV2.WithInternalService(sDelete),
		deletesvcV2.WithKeyStorage(keyStorage),
	)

	// build service pipeline
	// grpc | <metrics> | acl | signature | response | split

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
		&c.key.PrivateKey,
		respSvc,
	)

	aclSvc := acl.New(
		acl.WithSenderClassifier(
			acl.NewSenderClassifier(
				c.log,
				irFetcher,
				c.cfgNetmap.wrapper,
			),
		),
		acl.WithContainerSource(
			c.cfgObject.cnrSource,
		),
		acl.WithNextService(signSvc),
		acl.WithLocalStorage(ls),
		acl.WithEACLValidatorOptions(
			eacl.WithEACLSource(c.cfgObject.eaclSource),
			eacl.WithLogger(c.log),
		),
		acl.WithNetmapState(c.cfgNetmap.state),
	)

	var firstSvc objectService.ServiceServer = aclSvc
	if c.metricsCollector != nil {
		firstSvc = objectService.NewMetricCollector(aclSvc, c.metricsCollector)
	}

	server := objectTransportGRPC.New(firstSvc)

	for _, srv := range c.cfgGRPC.servers {
		objectGRPC.RegisterObjectServiceServer(srv, server)
	}
}

type morphEACLFetcher struct {
	w *cntrwrp.Wrapper
}

type signedEACLTable eaclSDK.Table

func (s *signedEACLTable) ReadSignedData(buf []byte) ([]byte, error) {
	return (*eaclSDK.Table)(s).Marshal(buf)
}

func (s *signedEACLTable) SignedDataSize() int {
	// TODO: add eacl.Table.Size method
	return (*eaclSDK.Table)(s).ToV2().StableSize()
}

func (s *morphEACLFetcher) GetEACL(cid *cid.ID) (*eaclSDK.Table, error) {
	table, err := s.w.GetEACL(cid)
	if err != nil {
		return nil, err
	}

	sig := table.Signature()

	if err := signature.VerifyDataWithSource(
		(*signedEACLTable)(table),
		func() ([]byte, []byte) {
			return sig.Key(), sig.Sign()
		},
		signature.SignWithRFC6979(),
	); err != nil {
		return nil, fmt.Errorf("incorrect signature: %w", err)
	}

	return table, nil
}

type reputationClientConstructor struct {
	log *logger.Logger

	nmSrc netmap.Source

	netState netmap.State

	trustStorage *truststorage.Storage

	basicConstructor interface {
		Get(coreclient.NodeInfo) (client.Client, error)
	}
}

type reputationClient struct {
	coreclient.Client

	prm truststorage.UpdatePrm

	cons *reputationClientConstructor
}

func (c *reputationClient) submitResult(err error) {
	prm := c.prm
	prm.SetSatisfactory(err == nil)
	prm.SetEpoch(c.cons.netState.CurrentEpoch())

	c.cons.trustStorage.Update(prm)
}

func (c *reputationClient) PutObject(ctx context.Context, prm *client.PutObjectParams, opts ...client.CallOption) (*objectSDK.ID, error) {
	id, err := c.Client.PutObject(ctx, prm, opts...)

	c.submitResult(err)

	return id, err
}

func (c *reputationClient) DeleteObject(ctx context.Context, prm *client.DeleteObjectParams, opts ...client.CallOption) error {
	err := c.Client.DeleteObject(ctx, prm, opts...)

	c.submitResult(err)

	return err
}

func (c *reputationClient) GetObject(ctx context.Context, prm *client.GetObjectParams, opts ...client.CallOption) (*objectSDK.Object, error) {
	obj, err := c.Client.GetObject(ctx, prm, opts...)

	c.submitResult(err)

	return obj, err
}

func (c *reputationClient) GetObjectHeader(ctx context.Context, prm *client.ObjectHeaderParams, opts ...client.CallOption) (*objectSDK.Object, error) {
	obj, err := c.Client.GetObjectHeader(ctx, prm, opts...)

	c.submitResult(err)

	return obj, err
}

func (c *reputationClient) ObjectPayloadRangeData(ctx context.Context, prm *client.RangeDataParams, opts ...client.CallOption) ([]byte, error) {
	rng, err := c.Client.ObjectPayloadRangeData(ctx, prm, opts...)

	c.submitResult(err)

	return rng, err
}

func (c *reputationClient) ObjectPayloadRangeSHA256(ctx context.Context, prm *client.RangeChecksumParams, opts ...client.CallOption) ([][sha256.Size]byte, error) {
	hashes, err := c.Client.ObjectPayloadRangeSHA256(ctx, prm, opts...)

	c.submitResult(err)

	return hashes, err
}

func (c *reputationClient) ObjectPayloadRangeTZ(ctx context.Context, prm *client.RangeChecksumParams, opts ...client.CallOption) ([][client.TZSize]byte, error) {
	hashes, err := c.Client.ObjectPayloadRangeTZ(ctx, prm, opts...)

	c.submitResult(err)

	return hashes, err
}

func (c *reputationClient) SearchObject(ctx context.Context, prm *client.SearchObjectParams, opts ...client.CallOption) ([]*objectSDK.ID, error) {
	ids, err := c.Client.SearchObject(ctx, prm, opts...)

	c.submitResult(err)

	return ids, err
}

func (c *reputationClientConstructor) Get(info coreclient.NodeInfo) (client.Client, error) {
	cl, err := c.basicConstructor.Get(info)
	if err != nil {
		return nil, err
	}

	nm, err := netmap.GetLatestNetworkMap(c.nmSrc)
	if err == nil {
		key := info.PublicKey()

		for i := range nm.Nodes {
			if bytes.Equal(nm.Nodes[i].PublicKey(), key) {
				prm := truststorage.UpdatePrm{}
				prm.SetPeer(reputation.PeerIDFromBytes(nm.Nodes[i].PublicKey()))

				return &reputationClient{
					Client: cl.(coreclient.Client),
					prm:    prm,
					cons:   c,
				}, nil
			}
		}
	} else {
		c.log.Warn("could not get latest network map to overload the client",
			zap.String("error", err.Error()),
		)
	}

	return cl, nil
}
