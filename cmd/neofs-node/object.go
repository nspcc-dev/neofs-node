package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
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
	"github.com/nspcc-dev/neofs-node/pkg/services/object/split"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/tombstone"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
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

	c.shared.replicator = replicator.New(
		replicator.WithLogger(c.log),
		replicator.WithPutTimeout(
			replicatorconfig.PutTimeout(c.cfgReader),
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, (*coreClientConstructor)(clientConstructor)),
		),
	)

	c.shared.policer = policer.New(
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
		policer.WithHeadTimeout(c.applicationConfiguration.policer.headTimeout),
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
		policer.WithMaxCapacity(c.applicationConfiguration.policer.maxCapacity),
		policer.WithPool(c.cfgObject.pool.replication),
		policer.WithNodeLoader(c),
		policer.WithNetwork(c),
		policer.WithReplicationCooldown(c.applicationConfiguration.policer.replicationCooldown),
		policer.WithObjectBatchSize(c.applicationConfiguration.policer.objectBatchSize),
	)

	traverseGen := util.NewTraverserGenerator(c.netMapSource, c.cfgObject.cnrSource, c)

	c.workers = append(c.workers, c.shared.policer)

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

	cnrNodes, err := newContainerNodes(c.cfgObject.cnrSource, c.netMapSource)
	fatalOnErr(err)

	sSearch := searchsvc.New(newRemoteContainerNodes(cnrNodes, c.IsLocalKey),
		searchsvc.WithLogger(c.log),
		searchsvc.WithLocalStorageEngine(ls),
		searchsvc.WithClientConstructor(coreConstructor),
		searchsvc.WithKeyStorage(keyStorage),
	)

	sSearchV2 := searchsvcV2.NewService(
		searchsvcV2.WithInternalService(sSearch),
		searchsvcV2.WithKeyStorage(keyStorage),
	)

	sPut := putsvc.NewService(
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(putConstructor),
		putsvc.WithMaxSizeSource(newCachedMaxObjectSizeSource(c)),
		putsvc.WithObjectStorage(storageEngine{engine: ls}),
		putsvc.WithContainerSource(c.cfgObject.cnrSource),
		putsvc.WithNetworkMapSource(c.netMapSource),
		putsvc.WithNetmapKeys(c),
		putsvc.WithNetworkState(c.cfgNetmap.state),
		putsvc.WithWorkerPools(c.cfgObject.pool.putRemote, c.cfgObject.pool.putLocal),
		putsvc.WithLogger(c.log),
		putsvc.WithSplitChainVerifier(split.NewVerifier(sGet)),
		putsvc.WithTombstoneVerifier(tombstone.NewVerifier(objectSource{sGet, sSearch})),
	)

	sPutV2 := putsvcV2.NewService(
		putsvcV2.WithInternalService(sPut),
		putsvcV2.WithKey(&c.key.PrivateKey),
	)

	sDelete := deletesvc.New(
		deletesvc.WithLogger(c.log),
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

	// cachedFirstObjectsNumber is a total cached objects number; the V2 split scheme
	// expects the first part of the chain to hold a user-defined header of the original
	// object which should be treated as a header to use for the eACL rules check; so
	// every object part in every chain will try to refer to the first part, so caching
	// should help a lot here
	const cachedFirstObjectsNumber = 1000

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
				SetLocalStorage(ls).
				SetHeaderSource(cachedHeaderSource(sGet, cachedFirstObjectsNumber)),
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

	objNode := newNodeForObjects(cnrNodes, sPut, c.IsLocalKey)

	server := objectTransportGRPC.New(firstSvc, objNode)

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
	log *zap.Logger

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

func (c *reputationClient) ObjectPutInit(ctx context.Context, hdr objectSDK.Object, signer user.Signer, prm client.PrmObjectPutInit) (client.ObjectWriter, error) {
	res, err := c.MultiAddressClient.ObjectPutInit(ctx, hdr, signer, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, writing errors are not processed
	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectDelete(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectDelete) (oid.ID, error) {
	res, err := c.MultiAddressClient.ObjectDelete(ctx, containerID, objectID, signer, prm)
	if err != nil {
		c.submitResult(err)
	}

	return res, err
}

func (c *reputationClient) GetObjectInit(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectGet) (objectSDK.Object, *client.PayloadReader, error) {
	hdr, rdr, err := c.MultiAddressClient.ObjectGetInit(ctx, containerID, objectID, signer, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, reading errors are not processed
	c.submitResult(err)

	return hdr, rdr, err
}

func (c *reputationClient) ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHead) (*objectSDK.Object, error) {
	res, err := c.MultiAddressClient.ObjectHead(ctx, containerID, objectID, signer, prm)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectHash(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHash) ([][]byte, error) {
	res, err := c.MultiAddressClient.ObjectHash(ctx, containerID, objectID, signer, prm)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectSearchInit(ctx context.Context, containerID cid.ID, signer user.Signer, prm client.PrmObjectSearch) (*client.ObjectListReader, error) {
	res, err := c.MultiAddressClient.ObjectSearchInit(ctx, containerID, signer, prm)

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

type storageEngine struct {
	engine *engine.StorageEngine
}

func (e storageEngine) IsLocked(address oid.Address) (bool, error) {
	return e.engine.IsLocked(address)
}

func (e storageEngine) Delete(tombstone oid.Address, toDelete []oid.ID) error {
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

func (e storageEngine) Lock(locker oid.Address, toLock []oid.ID) error {
	return e.engine.Lock(locker.Container(), locker.Object(), toLock)
}

func (e storageEngine) Put(o *objectSDK.Object) error {
	return engine.Put(e.engine, o)
}

func cachedHeaderSource(getSvc *getsvc.Service, cacheSize int) headerSource {
	hs := headerSource{getsvc: getSvc}

	if cacheSize > 0 {
		var err error
		hs.cache, err = lru.New[oid.Address, *objectSDK.Object](cacheSize)
		if err != nil {
			panic(fmt.Errorf("unexpected error in lru.New: %w", err))
		}
	}

	return hs
}

type headerSource struct {
	getsvc *getsvc.Service
	cache  *lru.Cache[oid.Address, *objectSDK.Object]
}

type headerWriter struct {
	h *objectSDK.Object
}

func (h *headerWriter) WriteHeader(o *objectSDK.Object) error {
	h.h = o
	return nil
}

func (h headerSource) Head(address oid.Address) (*objectSDK.Object, error) {
	if h.cache != nil {
		head, ok := h.cache.Get(address)
		if ok {
			return head, nil
		}
	}

	var hw headerWriter

	// no custom common prms since a caller is expected to be a container
	// participant so no additional headers, access tokens, etc
	var prm getsvc.HeadPrm
	prm.SetHeaderWriter(&hw)
	prm.WithAddress(address)
	prm.WithRawFlag(true)

	err := h.getsvc.Head(context.Background(), prm)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	h.cache.Add(address, hw.h)

	return hw.h, nil
}

type remoteContainerNodes struct {
	*containerNodes
	isLocalPubKey func([]byte) bool
}

func newRemoteContainerNodes(cnrNodes *containerNodes, isLocalPubKey func([]byte) bool) *remoteContainerNodes {
	return &remoteContainerNodes{
		containerNodes: cnrNodes,
		isLocalPubKey:  isLocalPubKey,
	}
}

// ForEachRemoteContainerNode iterates over all remote nodes matching the
// referenced container's storage policy in the current epoch and passes their
// descriptors into f. Elements may be repeated.
//
// Returns [apistatus.ErrContainerNotFound] if referenced container was not
// found.
//
// Implements [searchsvc.Containers] interface.
func (x *remoteContainerNodes) ForEachRemoteContainerNode(cnr cid.ID, f func(netmapsdk.NodeInfo)) error {
	return x.forEachContainerNode(cnr, false, func(node netmapsdk.NodeInfo) bool {
		if !x.isLocalPubKey(node.PublicKey()) {
			f(node)
		}
		return true
	})
}

// nodeForObjects represents NeoFS storage node for object storage.
type nodeForObjects struct {
	putObjectService *putsvc.Service
	containerNodes   *containerNodes
	isLocalPubKey    func([]byte) bool
}

func newNodeForObjects(cnrNodes *containerNodes, putObjectService *putsvc.Service, isLocalPubKey func([]byte) bool) *nodeForObjects {
	return &nodeForObjects{
		putObjectService: putObjectService,
		containerNodes:   cnrNodes,
		isLocalPubKey:    isLocalPubKey,
	}
}

// ForEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
//
// Implements [object.Node] interface.
func (x *nodeForObjects) ForEachContainerNodePublicKeyInLastTwoEpochs(id cid.ID, f func(pubKey []byte) bool) error {
	return x.containerNodes.forEachContainerNodePublicKeyInLastTwoEpochs(id, f)
}

// IsOwnPublicKey checks whether given binary-encoded public key is assigned to
// local storage node in the network map.
//
// Implements [object.Node] interface.
func (x *nodeForObjects) IsOwnPublicKey(pubKey []byte) bool {
	return x.isLocalPubKey(pubKey)
}

// VerifyAndStoreObject checks given object's format and, if it is correct,
// saves the object in the node's local object storage.
//
// Implements [object.Node] interface.
func (x *nodeForObjects) VerifyAndStoreObject(obj objectSDK.Object) error {
	return x.putObjectService.ValidateAndStoreObjectLocally(obj)
}

type objectSource struct {
	get    *getsvc.Service
	search *searchsvc.Service
}

type searchWriter struct {
	ids []oid.ID
}

func (w *searchWriter) WriteIDs(ids []oid.ID) error {
	w.ids = append(w.ids, ids...)
	return nil
}

func (o objectSource) Head(ctx context.Context, addr oid.Address) (*objectSDK.Object, error) {
	var hw headerWriter

	var hPrm getsvc.HeadPrm
	hPrm.SetHeaderWriter(&hw)
	hPrm.WithAddress(addr)
	hPrm.WithRawFlag(true)

	err := o.get.Head(ctx, hPrm)

	return hw.h, err
}

func (o objectSource) Search(ctx context.Context, cnr cid.ID, filters objectSDK.SearchFilters) ([]oid.ID, error) {
	var sw searchWriter

	var sPrm searchsvc.Prm
	sPrm.SetWriter(&sw)
	sPrm.WithSearchFilters(filters)
	sPrm.WithContainerID(cnr)

	err := o.search.Search(ctx, sPrm)
	if err != nil {
		return nil, err
	}

	return sw.ids, nil
}
