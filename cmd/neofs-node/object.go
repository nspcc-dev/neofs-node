package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	netmapClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/split"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/tombstone"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

type objectSvc struct {
	put *putsvc.Service

	search *searchsvc.Service

	get *getsvc.Service

	delete *deletesvc.Service
}

func (c *cfg) MaxObjectSize() uint64 {
	sz, err := c.nCli.MaxObjectSize()
	if err != nil {
		c.log.Error("could not get max object size value",
			zap.Error(err),
		)
	}

	return sz
}

func (s *objectSvc) Put(ctx context.Context) (*putsvc.Streamer, error) {
	return s.put.Put(ctx)
}

func (s *objectSvc) Head(ctx context.Context, prm getsvc.HeadPrm) error {
	return s.get.Head(ctx, prm)
}

func (s *objectSvc) Search(ctx context.Context, prm searchsvc.Prm) error {
	return s.search.Search(ctx, prm)
}

func (s *objectSvc) Get(ctx context.Context, prm getsvc.Prm) error {
	return s.get.Get(ctx, prm)
}

func (s *objectSvc) Delete(ctx context.Context, prm deletesvc.Prm) error {
	return s.delete.Delete(ctx, prm)
}

func (s *objectSvc) GetRange(ctx context.Context, prm getsvc.RangePrm) error {
	return s.get.GetRange(ctx, prm)
}

func (s *objectSvc) GetRangeHash(ctx context.Context, prm getsvc.RangeHashPrm) (*getsvc.RangeHashRes, error) {
	return s.get.GetRangeHash(ctx, prm)
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
			c.appCfg.Replicator.PutTimeout,
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, (*coreClientConstructor)(clientConstructor)),
		),
	)

	c.shared.policer = policer.New(
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithRemoteHeader(
			headsvc.NewRemoteHeader(keyStorage, clientConstructor),
		),
		policer.WithHeadTimeout(c.appCfg.Policer.HeadTimeout),
		policer.WithReplicator(c.replicator),
		policer.WithMaxCapacity(c.appCfg.Policer.MaxWorkers),
		policer.WithPool(c.cfgObject.pool.replication),
		policer.WithNodeLoader(c),
		policer.WithNetwork(c),
		policer.WithReplicationCooldown(c.appCfg.Policer.ReplicationCooldown),
		policer.WithObjectBatchSize(c.appCfg.Policer.ObjectBatchSize),
	)

	c.workers = append(c.workers, c.shared.policer)

	sGet := getsvc.New(c,
		getsvc.WithLogger(c.log),
		getsvc.WithLocalStorageEngine(ls),
		getsvc.WithClientConstructor(coreConstructor),
		getsvc.WithKeyStorage(keyStorage),
	)

	*c.cfgObject.getSvc = *sGet // need smth better

	cnrNodes, err := newContainerNodes(c.cnrSrc, c.netMapSource)
	fatalOnErr(err)
	c.cfgObject.containerNodes = cnrNodes

	sSearch := searchsvc.New(newRemoteContainerNodes(cnrNodes, c.IsLocalKey),
		searchsvc.WithLogger(c.log),
		searchsvc.WithLocalStorageEngine(ls),
		searchsvc.WithClientConstructor(coreConstructor),
		searchsvc.WithKeyStorage(keyStorage),
	)

	mNumber, err := c.shared.basics.cli.MagicNumber()
	fatalOnErr(err)

	os := &objectSource{get: sGet}
	sPut := putsvc.NewService(&transport{clients: putConstructor}, c, c.shared.metaService,
		putsvc.WithNetworkMagic(mNumber),
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(putConstructor),
		putsvc.WithContainerClient(c.cCli),
		putsvc.WithMaxSizeSource(newCachedMaxObjectSizeSource(c)),
		putsvc.WithObjectStorage(storageEngine{engine: ls}),
		putsvc.WithContainerSource(c.cnrSrc),
		putsvc.WithNetworkState(c.cfgNetmap.state),
		putsvc.WithRemoteWorkerPool(c.cfgObject.pool.putRemote),
		putsvc.WithLogger(c.log),
		putsvc.WithSplitChainVerifier(split.NewVerifier(sGet)),
		putsvc.WithTombstoneVerifier(tombstone.NewVerifier(os)),
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

	objSvc := &objectSvc{
		put:    sPut,
		search: sSearch,
		get:    sGet,
		delete: sDelete,
	}

	// cachedFirstObjectsNumber is a total cached objects number; the V2 split scheme
	// expects the first part of the chain to hold a user-defined header of the original
	// object which should be treated as a header to use for the eACL rules check; so
	// every object part in every chain will try to refer to the first part, so caching
	// should help a lot here
	const cachedFirstObjectsNumber = 1000
	fsChain := newFSChainForObjects(cnrNodes, c.IsLocalKey, c.networkState, c.cnrSrc, &c.isMaintenance, c.cli)

	aclSvc := v2.New(fsChain,
		v2.WithLogger(c.log),
		v2.WithIRFetcher(newCachedIRFetcher(irFetcher)),
		v2.WithNetmapper(netmapSourceWithNodes{
			Source:         c.netMapSource,
			fsChain:        fsChain,
			netmapContract: c.nCli,
		}),
		v2.WithContainerSource(c.cnrSrc),
	)
	addNewEpochAsyncNotificationHandler(c, func(event.Event) {
		aclSvc.ResetTokenCheckCache()
	})

	aclChecker := acl.NewChecker(new(acl.CheckerPrm).
		SetEACLSource(c.eaclSrc).
		SetValidator(eaclSDK.NewValidator()).
		SetLocalStorage(ls).
		SetHeaderSource(cachedHeaderSource(sGet, cachedFirstObjectsNumber, c.log)),
	)

	storage := storageForObjectService{
		local:   ls,
		metaSvc: c.shared.metaService,
		putSvc:  sPut,
		keys:    keyStorage,
	}
	server := objectService.New(objSvc, mNumber, fsChain, storage, c.metaService, c.shared.basics.key.PrivateKey, c.metricsCollector, aclChecker, aclSvc, coreConstructor)
	os.server = server

	for _, srv := range c.cfgGRPC.servers {
		protoobject.RegisterObjectServiceServer(srv, server)
	}
}

type reputationClientConstructor struct {
	log *zap.Logger

	nmSrc netmap.Source

	netState netmap.State

	trustStorage *truststorage.Storage

	basicConstructor interface {
		Get(coreclient.NodeInfo) (coreclient.MultiAddressClient, error)
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

	nm, err := c.nmSrc.NetMap()
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
					MultiAddressClient: cl,
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

func (e storageEngine) Put(o *objectSDK.Object, objBin []byte) error {
	return e.engine.Put(o, objBin)
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

type fsChainForObjects struct {
	containercore.Source
	netmap.StateDetailed
	containerNodes *containerNodes
	isLocalPubKey  func([]byte) bool
	isMaintenance  *atomic.Bool
	*morphClient.Client
}

func newFSChainForObjects(cnrNodes *containerNodes, isLocalPubKey func([]byte) bool, ns netmap.StateDetailed, cnrSource containercore.Source, isMaintenance *atomic.Bool, fsChainCli *morphClient.Client) *fsChainForObjects {
	return &fsChainForObjects{
		Source:         cnrSource,
		StateDetailed:  ns,
		containerNodes: cnrNodes,
		isLocalPubKey:  isLocalPubKey,
		isMaintenance:  isMaintenance,
		Client:         fsChainCli,
	}
}

// InContainerInLastTwoEpochs checks whether given public key belongs to any SN
// from the referenced container either in the current or the previous NeoFS
// epoch.
//
// Implements [v2.FSChain] interface.
func (x *fsChainForObjects) InContainerInLastTwoEpochs(cnr cid.ID, pub []byte) (bool, error) {
	var inContainer bool
	err := x.containerNodes.forEachContainerNodePublicKeyInLastTwoEpochs(cnr, func(nodePub []byte) bool {
		inContainer = bytes.Equal(nodePub, pub)
		return !inContainer
	})
	return inContainer, err
}

// ForEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
//
// Implements [object.Node] interface.
func (x *fsChainForObjects) ForEachContainerNodePublicKeyInLastTwoEpochs(id cid.ID, f func(pubKey []byte) bool) error {
	return x.containerNodes.forEachContainerNodePublicKeyInLastTwoEpochs(id, f)
}

// ForEachContainerNode implements [objectService.FSChain] interface.
func (x *fsChainForObjects) ForEachContainerNode(cnr cid.ID, f func(netmapsdk.NodeInfo) bool) error {
	return x.containerNodes.forEachContainerNode(cnr, false, f)
}

// IsOwnPublicKey checks whether given binary-encoded public key is assigned to
// local storage node in the network map.
//
// Implements [object.Node] interface.
func (x *fsChainForObjects) IsOwnPublicKey(pubKey []byte) bool {
	return x.isLocalPubKey(pubKey)
}

// LocalNodeUnderMaintenance checks whether local storage node is under
// maintenance now.
func (x *fsChainForObjects) LocalNodeUnderMaintenance() bool { return x.isMaintenance.Load() }

type storageForObjectService struct {
	local   *engine.StorageEngine
	metaSvc *meta.Meta
	putSvc  *putsvc.Service
	keys    *util.KeyStorage
}

// SearchObjects implements [objectService.Storage] interface.
func (x storageForObjectService) SearchObjects(cID cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	return x.local.Search(cID, fs, attrs, cursor, count)
}

func (x storageForObjectService) VerifyAndStoreObjectLocally(obj objectSDK.Object) error {
	return x.putSvc.ValidateAndStoreObjectLocally(obj)
}

func (x storageForObjectService) GetSessionPrivateKey(usr user.ID, uid uuid.UUID) (ecdsa.PrivateKey, error) {
	k, err := x.keys.GetKey(&util.SessionInfo{ID: uid, Owner: usr})
	if err != nil {
		return ecdsa.PrivateKey{}, err
	}
	return *k, nil
}

type objectSource struct {
	get    *getsvc.Service
	server interface {
		ProcessSearch(ctx context.Context, req *protoobject.SearchV2Request) ([]client.SearchResultItem, []byte, error)
	}
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

func (o objectSource) SearchOne(ctx context.Context, cnr cid.ID, filters objectSDK.SearchFilters) (oid.ID, error) {
	var id oid.ID
	res, _, err := o.server.ProcessSearch(ctx, &protoobject.SearchV2Request{
		Body: &protoobject.SearchV2Request_Body{
			ContainerId: cnr.ProtoMessage(),
			Version:     1,
			Filters:     filters.ProtoMessage(),
			Cursor:      "",
			Count:       1,
			Attributes:  nil,
		},
		MetaHeader: &protosession.RequestMetaHeader{
			Version: version.Current().ProtoMessage(),
			Ttl:     2,
		},
	})
	if err != nil {
		return id, err
	}

	if len(res) == 1 {
		return res[0].ID, nil
	}
	return id, nil
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
// GetNodesForObject implements [getsvc.NeoFSNetwork], [policer.Network].
func (c *cfg) GetNodesForObject(addr oid.Address) ([][]netmapsdk.NodeInfo, []uint, []iec.Rule, error) {
	nodeSets, repRules, ecRules, err := c.cfgObject.containerNodes.getNodesForObject(addr)
	return nodeSets, repRules, ecRules, err
}

type netmapSourceWithNodes struct {
	netmap.Source
	fsChain        *fsChainForObjects
	netmapContract *netmapClient.Client
}

func (n netmapSourceWithNodes) ServerInContainer(cID cid.ID) (bool, error) {
	var serverInContainer bool
	err := n.fsChain.ForEachContainerNodePublicKeyInLastTwoEpochs(cID, func(pubKey []byte) bool {
		if n.fsChain.isLocalPubKey(pubKey) {
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

func (n netmapSourceWithNodes) GetEpochBlock(epoch uint64) (uint32, error) {
	return n.netmapContract.GetEpochBlock(epoch)
}

func (c *cfg) GetEpochBlock(epoch uint64) (uint32, error) {
	return c.nCli.GetEpochBlock(epoch)
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
	policy := cnr.PlacementPolicy()
	nodeSets, err := networkMap.ContainerNodes(policy, cnrID)
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
func (x *containerNodesSorter) ECRules() []iec.Rule              { return x.policy.ecRules }
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
	res.ecRules = x.policy.ecRules
	res.nodeSets, res.err = x.containerNodes.sortContainerNodesFunc(*x.networkMap, x.policy.nodeSets, obj)
	if res.err != nil {
		res.err = fmt.Errorf("sort container nodes for object: %w", res.err)
	}
	x.containerNodes.objCache.Add(cacheKey, res)
	return res.nodeSets, res.err
}
