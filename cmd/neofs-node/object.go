package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	policerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/policer"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
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
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
func (i *delNetInfo) LocalNodeID() user.ID {
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

type coreClientConstructor reputationClientConstructor

func (x *coreClientConstructor) Get(info coreclient.NodeInfo) (coreclient.MultiAddressClient, error) {
	c, err := (*reputationClientConstructor)(x).Get(info)
	if err != nil {
		return nil, err
	}

	return c.(coreclient.MultiAddressClient), nil
}

// IsLocalNodeInNetmap looks for the local node in the latest view of the
// network map.
func (c *cfg) IsLocalNodeInNetmap() bool {
	return c.localNodeInNetmap.Load()
}

func initObjectService(c *cfg) {
	ls := c.cfgObject.cfgLocalStorage.localStorage
	keyStorage := util.NewKeyStorage(&c.key.PrivateKey, c.privateTokenStore, c.cfgNetmap.state)

	clientConstructor := &reputationClientConstructor{
		log:              c.log,
		nmSrc:            c.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.bgClientCache,
	}

	coreConstructor := &coreClientConstructor{
		log:              c.log,
		nmSrc:            c.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.clientCache,
	}

	putConstructor := &coreClientConstructor{
		log:              c.log,
		nmSrc:            c.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.putClientCache,
	}

	irFetcher := &innerRingFetcherWithNotary{
		sidechain: c.cfgMorph.client,
	}

	c.replicator = replicator.New(
		replicator.WithLogger(c.log),
		replicator.WithPutTimeout(
			replicatorconfig.PutTimeout(c.appCfg),
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, (*coreClientConstructor)(clientConstructor)),
		),
	)

	pol := policer.New(
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithContainerSource(c.cfgObject.cnrSource),
		policer.WithPlacementBuilder(
			placement.NewNetworkMapSourceBuilder(c.netMapSource),
		),
		policer.WithRemoteHeader(
			headsvc.NewRemoteHeader(keyStorage, clientConstructor),
		),
		policer.WithNetmapKeys(c),
		policer.WithHeadTimeout(
			policerconfig.HeadTimeout(c.appCfg),
		),
		policer.WithReplicator(c.replicator),
		policer.WithRedundantCopyCallback(func(addr oid.Address) {
			var inhumePrm engine.InhumePrm
			inhumePrm.MarkAsGarbage(addr)

			_, err := ls.Inhume(inhumePrm)
			if err != nil {
				c.log.Warn("could not inhume mark redundant copy as garbage",
					zap.String("error", err.Error()),
				)
			}
		}),
		policer.WithMaxCapacity(c.cfgObject.pool.replicatorPoolSize),
		policer.WithPool(c.cfgObject.pool.replication),
		policer.WithNodeLoader(c),
		policer.WithNetwork(c),
	)

	traverseGen := util.NewTraverserGenerator(c.netMapSource, c.cfgObject.cnrSource, c)

	c.workers = append(c.workers, pol)

	var os putsvc.ObjectStorage = engineWithoutNotifications{
		engine: ls,
	}

	if c.cfgNotifications.enabled {
		os = engineWithNotifications{
			base:         os,
			nw:           c.cfgNotifications.nw,
			ns:           c.cfgNetmap.state,
			defaultTopic: c.cfgNotifications.defaultTopic,
		}
	}

	sPut := putsvc.NewService(
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(putConstructor),
		putsvc.WithMaxSizeSource(newCachedMaxObjectSizeSource(c)),
		putsvc.WithObjectStorage(os),
		putsvc.WithContainerSource(c.cfgObject.cnrSource),
		putsvc.WithNetworkMapSource(c.netMapSource),
		putsvc.WithNetmapKeys(c),
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
		searchsvc.WithNetMapSource(c.netMapSource),
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
		getsvc.WithNetMapSource(c.netMapSource),
		getsvc.WithKeyStorage(keyStorage),
	)

	*c.cfgObject.getSvc = *sGet // need smth better

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
			tsLifetime: c.cfgObject.tombstoneLifetime,

			cfg: c,
		}),
		deletesvc.WithKeyStorage(keyStorage),
	)

	sDeleteV2 := deletesvcV2.NewService(
		deletesvcV2.WithInternalService(sDelete),
	)

	// build service pipeline
	// grpc | <metrics> | signature | response | acl | split

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

	aclSvc := v2.New(
		v2.WithLogger(c.log),
		v2.WithIRFetcher(newCachedIRFetcher(irFetcher)),
		v2.WithNetmapSource(c.netMapSource),
		v2.WithContainerSource(
			c.cfgObject.cnrSource,
		),
		v2.WithNextService(splitSvc),
		v2.WithEACLChecker(
			acl.NewChecker(new(acl.CheckerPrm).
				SetNetmapState(c.cfgNetmap.state).
				SetEACLSource(c.cfgObject.eaclSource).
				SetValidator(eaclSDK.NewValidator()).
				SetLocalStorage(ls),
			),
		),
	)

	var commonSvc objectService.Common
	commonSvc.Init(&c.internals, aclSvc)

	respSvc := objectService.NewResponseService(
		&commonSvc,
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
	w *cntClient.Client
}

func (s *morphEACLFetcher) GetEACL(cnr cid.ID) (*containercore.EACL, error) {
	eaclInfo, err := s.w.GetEACL(cnr)
	if err != nil {
		return nil, err
	}

	binTable, err := eaclInfo.Value.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal eACL table: %w", err)
	}

	if !eaclInfo.Signature.Verify(binTable) {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("invalid signature of the eACL table")
	}

	return eaclInfo, nil
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
	currEpoch := c.cons.netState.CurrentEpoch()
	sat := err == nil

	c.cons.log.Debug(
		"writing local reputation values",
		zap.Uint64("epoch", currEpoch),
		zap.Bool("satisfactory", sat),
	)

	prm := c.prm
	prm.SetSatisfactory(sat)
	prm.SetEpoch(currEpoch)

	c.cons.trustStorage.Update(prm)
}

func (c *reputationClient) ObjectPutInit(ctx context.Context, prm client.PrmObjectPutInit) (*client.ObjectWriter, error) {
	res, err := c.MultiAddressClient.ObjectPutInit(ctx, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, writing errors are not processed
	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectDelete(ctx context.Context, containerID cid.ID, objectID oid.ID, prm client.PrmObjectDelete) (oid.ID, error) {
	res, err := c.MultiAddressClient.ObjectDelete(ctx, containerID, objectID, prm)
	if err != nil {
		c.submitResult(err)
	}

	return res, err
}

func (c *reputationClient) GetObjectInit(ctx context.Context, containerID cid.ID, objectID oid.ID, prm client.PrmObjectGet) (*client.ObjectReader, error) {
	res, err := c.MultiAddressClient.ObjectGetInit(ctx, containerID, objectID, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, reading errors are not processed
	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, prm client.PrmObjectHead) (*client.ResObjectHead, error) {
	res, err := c.MultiAddressClient.ObjectHead(ctx, containerID, objectID, prm)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectHash(ctx context.Context, containerID cid.ID, objectID oid.ID, prm client.PrmObjectHash) ([][]byte, error) {
	res, err := c.MultiAddressClient.ObjectHash(ctx, containerID, objectID, prm)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectSearchInit(ctx context.Context, containerID cid.ID, prm client.PrmObjectSearch) (*client.ObjectListReader, error) {
	res, err := c.MultiAddressClient.ObjectSearchInit(ctx, containerID, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, reading errors are not processed
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

		nmNodes := nm.Nodes()
		var peer apireputation.PeerID

		for i := range nmNodes {
			if bytes.Equal(nmNodes[i].PublicKey(), key) {
				peer.SetPublicKey(nmNodes[i].PublicKey())

				prm := truststorage.UpdatePrm{}
				prm.SetPeer(peer)

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

type engineWithNotifications struct {
	base putsvc.ObjectStorage
	nw   notificationWriter
	ns   netmap.State

	defaultTopic string
}

func (e engineWithNotifications) Delete(tombstone oid.Address, toDelete []oid.ID) error {
	return e.base.Delete(tombstone, toDelete)
}

func (e engineWithNotifications) Lock(locker oid.Address, toLock []oid.ID) error {
	return e.base.Lock(locker, toLock)
}

func (e engineWithNotifications) Put(o *objectSDK.Object) error {
	if err := e.base.Put(o); err != nil {
		return err
	}

	ni, err := o.NotificationInfo()
	if err == nil {
		if epoch := ni.Epoch(); epoch == 0 || epoch == e.ns.CurrentEpoch() {
			topic := ni.Topic()

			if topic == "" {
				topic = e.defaultTopic
			}

			e.nw.Notify(topic, objectCore.AddressOf(o))
		}
	}

	return nil
}

type engineWithoutNotifications struct {
	engine *engine.StorageEngine
}

func (e engineWithoutNotifications) Delete(tombstone oid.Address, toDelete []oid.ID) error {
	var prm engine.InhumePrm

	addrs := make([]oid.Address, len(toDelete))
	for i := range addrs {
		addrs[i].SetContainer(tombstone.Container())
		addrs[i].SetObject(toDelete[i])
	}

	prm.WithTarget(tombstone, addrs...)

	_, err := e.engine.Inhume(prm)
	return err
}

func (e engineWithoutNotifications) Lock(locker oid.Address, toLock []oid.ID) error {
	return e.engine.Lock(locker.Container(), locker.Object(), toLock)
}

func (e engineWithoutNotifications) Put(o *objectSDK.Object) error {
	return engine.Put(e.engine, o)
}
