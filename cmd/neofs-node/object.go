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
			zap.Error(err),
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

// LocalNodeID returns node owner ID calculated from configured private key.
//
// Implements method needed for Object.Delete service.
func (i *delNetInfo) LocalNodeID() user.ID {
	return i.cfg.ownerIDFromKey
}

type innerRingFetcherWithNotary struct {
	fschain *morphClient.Client
}

func (fn *innerRingFetcherWithNotary) InnerRingKeys() ([][]byte, error) {
	keys, err := fn.fschain.NeoFSAlphabetList()
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
		fschain: c.cfgMorph.client,
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
			err := ls.Delete(addr)
			if err != nil {
				c.log.Warn("could not inhume mark redundant copy as garbage",
					zap.Error(err),
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

	c.workers = append(c.workers, c.shared.policer)

	sGet := getsvc.New(c,
		getsvc.WithLogger(c.log),
		getsvc.WithLocalStorageEngine(ls),
		getsvc.WithClientConstructor(coreConstructor),
		getsvc.WithKeyStorage(keyStorage),
	)

	*c.cfgObject.getSvc = *sGet // need smth better

	sGetV2 := getsvcV2.NewService(
		getsvcV2.WithInternalService(sGet),
		getsvcV2.WithKeyStorage(keyStorage),
	)

	cnrNodes, err := newContainerNodes(c.cfgObject.cnrSource, c.netMapSource)
	fatalOnErr(err)
	c.cfgObject.containerNodes = cnrNodes

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

	mNumber, err := c.shared.basics.cli.MagicNumber()
	fatalOnErr(err)

	sPut := putsvc.NewService(&transport{clients: putConstructor}, c,
		putsvc.WithNetworkMagic(mNumber),
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(putConstructor),
		putsvc.WithMaxSizeSource(newCachedMaxObjectSizeSource(c)),
		putsvc.WithObjectStorage(storageEngine{engine: ls}),
		putsvc.WithContainerSource(c.cfgObject.cnrSource),
		putsvc.WithNetworkMapSource(c.netMapSource),
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
	// grpc | response | acl | split

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
	objNode := newNodeForObjects(cnrNodes, sPut, c.IsLocalKey)

	aclSvc := v2.New(
		v2.WithLogger(c.log),
		v2.WithIRFetcher(newCachedIRFetcher(irFetcher)),
		v2.WithNetmapper(netmapSourceWithNodes{Source: c.netMapSource, n: objNode}),
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
				SetHeaderSource(cachedHeaderSource(sGet, cachedFirstObjectsNumber, c.log)),
			),
		),
	)

	var commonSvc objectService.Common
	commonSvc.Init(&c.internals, aclSvc)

	respSvc := objectService.NewResponseService(
		&commonSvc,
		c.respSvc,
	)

	server := objectService.New(respSvc, mNumber, objNode, c.key.PrivateKey, c.cfgNetmap.state, c.metricsCollector)

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

	if !eaclInfo.Signature.Verify(eaclInfo.Value.Marshal()) {
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
			zap.Error(err),
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

func (e storageEngine) Delete(tombstone oid.Address, tombExpiration uint64, toDelete []oid.ID) error {
	addrs := make([]oid.Address, len(toDelete))
	for i := range addrs {
		addrs[i].SetContainer(tombstone.Container())
		addrs[i].SetObject(toDelete[i])
	}

	return e.engine.Inhume(tombstone, tombExpiration, addrs...)
}

func (e storageEngine) Lock(locker oid.Address, toLock []oid.ID) error {
	return e.engine.Lock(locker.Container(), locker.Object(), toLock)
}

func (e storageEngine) Put(o *objectSDK.Object, objBin []byte, hdrLen int) error {
	return e.engine.Put(o, objBin, hdrLen)
}

func cachedHeaderSource(getSvc *getsvc.Service, cacheSize int, l *zap.Logger) headerSource {
	hs := headerSource{
		getsvc: getSvc,
		l:      l.With(zap.String("service", "cached header source"), zap.Int("cache capacity", cacheSize)),
	}

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
	l      *zap.Logger
}

type headerWriter struct {
	h *objectSDK.Object
}

func (h *headerWriter) WriteHeader(o *objectSDK.Object) error {
	h.h = o
	return nil
}

func (h headerSource) Head(address oid.Address) (*objectSDK.Object, error) {
	l := h.l.With(zap.Stringer("address", address))
	l.Debug("requesting header")

	if h.cache != nil {
		head, ok := h.cache.Get(address)
		if ok {
			l.Debug("returning header from cache")
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

	l.Debug("returning header from network")

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

// IsLocalNodePublicKey checks whether given binary-encoded public key is
// assigned in the network map to a local storage node.
//
// IsLocalNodePublicKey implements [getsvc.NeoFSNetwork].
func (c *cfg) IsLocalNodePublicKey(b []byte) bool { return c.IsLocalKey(b) }

// GetNodesForObject reads storage policy of the referenced container from the
// underlying container storage, reads network map at the specified epoch from
// the underlying storage, applies the storage policy to it and returns sorted
// lists of selected storage nodes along with the per-list numbers of primary
// object holders. Resulting slices must not be changed.
//
// GetNodesForObject implements [getsvc.NeoFSNetwork].
func (c *cfg) GetNodesForObject(addr oid.Address) ([][]netmapsdk.NodeInfo, []uint, error) {
	return c.cfgObject.containerNodes.getNodesForObject(addr)
}

type netmapSourceWithNodes struct {
	netmap.Source
	n *nodeForObjects
}

func (n netmapSourceWithNodes) ServerInContainer(cID cid.ID) (bool, error) {
	var serverInContainer bool
	err := n.n.ForEachContainerNodePublicKeyInLastTwoEpochs(cID, func(pubKey []byte) bool {
		if n.n.isLocalPubKey(pubKey) {
			serverInContainer = true
			return false
		}
		return true
	})
	if err != nil {
		if errors.Is(err, netmapsdk.ErrNotEnoughNodes) {
			return false, nil
		}

		return false, err
	}

	return serverInContainer, nil
}

// GetContainerNodes reads storage policy of the referenced container from the
// underlying container storage, reads current network map from the underlying
// storage, applies the storage policy to it, gathers storage nodes matching the
// policy and returns sort interface.
//
// GetContainerNodes implements [putsvc.NeoFSNetwork].
func (c *cfg) GetContainerNodes(cnrID cid.ID) (putsvc.ContainerNodes, error) {
	cnr, err := c.cfgObject.containerNodes.containers.Get(cnrID)
	if err != nil {
		return nil, fmt.Errorf("read container by ID: %w", err)
	}
	curEpoch, err := c.cfgObject.containerNodes.network.Epoch()
	if err != nil {
		return nil, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	networkMap, err := c.cfgObject.containerNodes.network.GetNetMapByEpoch(curEpoch)
	if err != nil {
		return nil, fmt.Errorf("read network map at the current epoch #%d: %w", curEpoch, err)
	}
	policy := cnr.Value.PlacementPolicy()
	nodeSets, err := networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), cnrID)
	if err != nil {
		return nil, fmt.Errorf("apply container storage policy to the network map at current epoch #%d: %w", curEpoch, err)
	}
	repCounts := make([]uint, policy.NumberOfReplicas())
	for i := range repCounts {
		repCounts[i] = uint(policy.ReplicaNumberByIndex(i))
	}
	return &containerNodesSorter{
		policy: storagePolicyRes{
			nodeSets:  nodeSets,
			repCounts: repCounts,
		},
		networkMap:     networkMap,
		cnrID:          cnrID,
		curEpoch:       curEpoch,
		containerNodes: c.cfgObject.containerNodes,
	}, nil
}

// implements [putsvc.ContainerNodes].
type containerNodesSorter struct {
	policy         storagePolicyRes
	networkMap     *netmapsdk.NetMap
	cnrID          cid.ID
	curEpoch       uint64
	containerNodes *containerNodes
}

func (x *containerNodesSorter) Unsorted() [][]netmapsdk.NodeInfo { return x.policy.nodeSets }
func (x *containerNodesSorter) PrimaryCounts() []uint            { return x.policy.repCounts }
func (x *containerNodesSorter) SortForObject(obj oid.ID) ([][]netmapsdk.NodeInfo, error) {
	cacheKey := objectNodesCacheKey{epoch: x.curEpoch}
	cacheKey.addr.SetContainer(x.cnrID)
	cacheKey.addr.SetObject(obj)
	res, ok := x.containerNodes.objCache.Get(cacheKey)
	if ok {
		return res.nodeSets, res.err
	}
	if x.networkMap == nil {
		var err error
		if x.networkMap, err = x.containerNodes.network.GetNetMapByEpoch(x.curEpoch); err != nil {
			// non-persistent error => do not cache
			return nil, fmt.Errorf("read network map by epoch: %w", err)
		}
	}
	res.repCounts = x.policy.repCounts
	res.nodeSets, res.err = x.containerNodes.sortContainerNodesFunc(*x.networkMap, x.policy.nodeSets, obj)
	if res.err != nil {
		res.err = fmt.Errorf("sort container nodes for object: %w", res.err)
	}
	x.containerNodes.objCache.Add(cacheKey, res)
	return res.nodeSets, res.err
}
