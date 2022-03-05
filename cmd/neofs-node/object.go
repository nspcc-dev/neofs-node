package main

import (
	"bytes"
	"context"
	"fmt"

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
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
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
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/util/signature"
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

	cfg *cfg
}

func (i *delNetInfo) TombstoneLifetime() (uint64, error) {
	return i.tsLifetime, nil
}

// returns node owner ID calculated from configured private key.
//
// Implements method needed for Object.Delete service.
func (i *delNetInfo) LocalNodeID() *owner.ID {
	return i.cfg.ownerIDFromKey
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

func (x *coreClientConstructor) Get(info coreclient.NodeInfo) (coreclient.MultiAddressClient, error) {
	c, err := (*reputationClientConstructor)(x).Get(info)
	if err != nil {
		return nil, err
	}

	return c.(coreclient.MultiAddressClient), nil
}

func initObjectService(c *cfg) {
	ls := c.cfgObject.cfgLocalStorage.localStorage
	keyStorage := util.NewKeyStorage(&c.key.PrivateKey, c.privateTokenStore, c.cfgNetmap.state)

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

	pol := policer.New(
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithContainerSource(c.cfgObject.cnrSource),
		policer.WithPlacementBuilder(
			placement.NewNetworkMapSourceBuilder(c.cfgObject.netMapSource),
		),
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
		policer.WithMaxCapacity(c.cfgObject.pool.putRemoteCapacity),
		policer.WithPool(c.cfgObject.pool.replication),
		policer.WithNodeLoader(c),
	)

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
		searchsvc.WithKeyStorage(keyStorage),
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
		getsvc.WithKeyStorage(keyStorage),
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

			cfg: c,
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
		acl.WithNextService(splitSvc),
		acl.WithLocalStorage(ls),
		acl.WithEACLSource(c.cfgObject.eaclSource),
		acl.WithNetmapState(c.cfgNetmap.state),
	)

	respSvc := objectService.NewResponseService(
		aclSvc,
		c.respSvc,
	)

	signSvc := objectService.NewSignService(
		&c.key.PrivateKey,
		respSvc,
	)

	var firstSvc objectService.ServiceServer = signSvc
	if c.metricsCollector != nil {
		firstSvc = objectService.NewMetricCollector(signSvc, c.metricsCollector)
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

func (s *signedEACLTable) ReadSignedData(_ []byte) ([]byte, error) {
	return (*eaclSDK.Table)(s).Marshal()
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
		Get(coreclient.NodeInfo) (coreclient.Client, error)
	}
}

type reputationClient struct {
	coreclient.MultiAddressClient

	prm truststorage.UpdatePrm

	cons *reputationClientConstructor
}

func (c *reputationClient) submitResult(err error) {
	prm := c.prm
	prm.SetSatisfactory(err == nil)
	prm.SetEpoch(c.cons.netState.CurrentEpoch())

	c.cons.trustStorage.Update(prm)
}

func (c *reputationClient) PutObject(ctx context.Context, prm *client.PutObjectParams, opts ...client.CallOption) (*client.ObjectPutRes, error) {
	res, err := c.MultiAddressClient.PutObject(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) DeleteObject(ctx context.Context, prm *client.DeleteObjectParams, opts ...client.CallOption) (*client.ObjectDeleteRes, error) {
	res, err := c.MultiAddressClient.DeleteObject(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) GetObject(ctx context.Context, prm *client.GetObjectParams, opts ...client.CallOption) (*client.ObjectGetRes, error) {
	res, err := c.MultiAddressClient.GetObject(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) HeadObject(ctx context.Context, prm *client.ObjectHeaderParams, opts ...client.CallOption) (*client.ObjectHeadRes, error) {
	res, err := c.MultiAddressClient.HeadObject(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectPayloadRangeData(ctx context.Context, prm *client.RangeDataParams, opts ...client.CallOption) (*client.ObjectRangeRes, error) {
	res, err := c.MultiAddressClient.ObjectPayloadRangeData(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) HashObjectPayloadRanges(ctx context.Context, prm *client.RangeChecksumParams, opts ...client.CallOption) (*client.ObjectRangeHashRes, error) {
	res, err := c.MultiAddressClient.HashObjectPayloadRanges(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) SearchObjects(ctx context.Context, prm *client.SearchObjectParams, opts ...client.CallOption) (*client.ObjectSearchRes, error) {
	res, err := c.MultiAddressClient.SearchObjects(ctx, prm, opts...)

	c.submitResult(err)

	return res, err
}

func (c *reputationClientConstructor) Get(info coreclient.NodeInfo) (coreclient.Client, error) {
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
					MultiAddressClient: cl.(coreclient.MultiAddressClient),
					prm:                prm,
					cons:               c,
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
