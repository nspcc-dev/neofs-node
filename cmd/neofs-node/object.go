package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	nnscore "github.com/nspcc-dev/neofs-node/pkg/core/nns"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/placement"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/split"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/tombstone"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type objectSvc struct {
	put *putsvc.Service

	get *getsvc.Service

	delete *deletesvc.Service
}

type putPostInitialPlacementReplicator struct {
	log        *zap.Logger
	replicator *replicator.Replicator
	disabled   bool
}

func (r putPostInitialPlacementReplicator) HandlePostPlacement(obj *object.Object, nodes []netmap.NodeInfo) {
	if r.disabled {
		r.log.Debug("post-placement replication is disabled",
			zap.Stringer("object", obj.Address()),
		)
		return
	}

	copies := uint32(len(nodes))
	if pi, err := iec.GetPartInfo(*obj); err == nil && pi.Index >= 0 {
		copies = 1
	}

	var task replicator.Task
	task.SetObjectAddress(obj.Address())
	task.SetObject(obj)
	task.SetNodes(nodes)
	task.SetCopiesNumber(copies)

	if err := r.replicator.EnqueueTask(task); err != nil {
		r.log.Warn("could not enqueue post-placement replication",
			zap.Stringer("object", obj.Address()),
			zap.Uint32("expected", copies),
			zap.Error(err),
		)
	}
}

func (c *cfg) MaxObjectSize() (uint64, error) {
	sz, err := c.nCli.MaxObjectSize()
	if err != nil {
		c.log.Error("could not get max object size value",
			zap.Error(err),
		)
	}

	return sz, err
}

func (s *objectSvc) Put(ctx context.Context) (*putsvc.Streamer, error) {
	return s.put.Put(ctx)
}

func (s *objectSvc) Head(ctx context.Context, prm getsvc.HeadPrm) error {
	return s.get.Head(ctx, prm)
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

type delNetInfo struct {
	netmapcore.State
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
	cfg     *cfg
	fschain *morphClient.Client
}

func (fn *innerRingFetcherWithNotary) InnerRingKeys() ([][]byte, error) {
	keys, err := fn.fschain.NeoFSAlphabetList()
	if err != nil {
		fn.cfg.log.Error("can't get inner ring key list", zap.Error(err))
		return nil, fmt.Errorf("can't get inner ring key list: %w", err)
	}

	result := make([][]byte, 0, len(keys))
	for i := range keys {
		result = append(result, keys[i].Bytes())
	}

	return result, nil
}

type coreClientConstructor reputationClientConstructor

func (x *coreClientConstructor) Get(ctx context.Context, info netmap.NodeInfo) (clientcore.MultiAddressClient, error) {
	c, err := (*reputationClientConstructor)(x).Get(ctx, info)
	if err != nil {
		return nil, err
	}

	return c.(clientcore.MultiAddressClient), nil
}

// IsLocalNodeInNetmap looks for the local node in the latest view of the
// network map.
func (c *cfg) IsLocalNodeInNetmap() bool {
	return c.localNodeInNetmap.Load()
}

func initObjectService(c *cfg) {
	ls := c.cfgObject.cfgLocalStorage.localStorage
	keyStorage := util.NewKeyStorage(&c.key.PrivateKey, c.privateTokenStore, c.cfgNetmap.state)

	repClientConstructor := &reputationClientConstructor{
		log:              c.log,
		nmSrc:            c.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.clientCache,
	}

	coreConstructor := &coreClientConstructor{
		log:              c.log,
		nmSrc:            c.netMapSource,
		netState:         c.cfgNetmap.state,
		trustStorage:     c.cfgReputation.localTrustStorage,
		basicConstructor: c.clientCache,
	}

	irFetcher := &innerRingFetcherWithNotary{
		cfg:     c,
		fschain: c.cfgMorph.client,
	}

	c.replicator = replicator.New(
		replicator.WithLogger(c.log),
		replicator.WithPutTimeout(
			c.appCfg.Replicator.PutTimeout,
		),
		replicator.WithLocalStorage(ls),
		replicator.WithRemoteSender(
			putsvc.NewRemoteSender(keyStorage, (*coreClientConstructor)(repClientConstructor)),
		),
		replicator.WithLocalNodeKey(c),
	)

	c.policer = policer.New(neofsecdsa.Signer(c.key.PrivateKey),
		policer.WithLogger(c.log),
		policer.WithLocalStorage(ls),
		policer.WithMetrics(c),
		policer.WithRemoteHeader(
			headsvc.NewRemoteHeader(keyStorage, repClientConstructor),
		),
		policer.WithHeadTimeout(c.appCfg.Policer.HeadTimeout),
		policer.WithReplicator(c.replicator),
		policer.WithNetwork(c),
		policer.WithReplicationCooldown(c.appCfg.Policer.ReplicationCooldown),
		policer.WithObjectBatchSize(c.appCfg.Policer.ObjectBatchSize),
		policer.WithBoostMultiplier(c.appCfg.Policer.BoostMultiplier),
	)

	c.workers = append(c.workers, c.policer, c.replicator)

	nnsResolver := nnscore.NewResolver(c.cli)

	sGet := getsvc.New(c,
		getsvc.WithLogger(c.log),
		getsvc.WithLocalStorageEngine(ls),
		getsvc.WithClientConstructor(coreConstructor),
		getsvc.WithKeyStorage(keyStorage),
		getsvc.WithNNSResolver(nnsResolver),
	)

	*c.cfgObject.getSvc = *sGet // need smth better

	placementSvc, err := placement.New(c.cnrSrc, c.netMapSource)
	fatalOnErr(err)
	c.cfgObject.placement = placementSvc

	os := &objectSource{signer: neofsecdsa.SignerRFC6979(c.key.PrivateKey), get: sGet}
	sPut := putsvc.NewService(&transport{clients: coreConstructor}, c, c.metaService,
		initQuotas(c.cCli, c.cfgObject.quotasTTL),
		c.containerPayments,
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithClientConstructor(coreConstructor),
		putsvc.WithContainerClient(c.cCli),
		putsvc.WithMaxSizeSource(newCachedMaxObjectSizeSource(c.MaxObjectSize)),
		putsvc.WithObjectStorage(ls),
		putsvc.WithContainerSource(c.cnrSrc),
		putsvc.WithNetworkState(c.cfgNetmap.state),
		putsvc.WithRemoteWorkerPool(c.cfgObject.pool.putRemote),
		putsvc.WithPostPlacementReplicator(putPostInitialPlacementReplicator{
			log:        c.log,
			replicator: c.replicator,
			disabled:   c.appCfg.Replicator.DisablePostInitialQueue,
		}),
		putsvc.WithLogger(c.log),
		putsvc.WithSplitChainVerifier(split.NewVerifier(sGet)),
		putsvc.WithTombstoneVerifier(tombstone.NewVerifier(os)),
		putsvc.WithNNSResolver(nnsResolver),
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
		deletesvc.WithNNSResolver(nnsResolver),
	)

	objSvc := &objectSvc{
		put:    sPut,
		get:    sGet,
		delete: sDelete,
	}

	// cachedFirstObjectsNumber is a total cached objects number; the V2 split scheme
	// expects the first part of the chain to hold a user-defined header of the original
	// object which should be treated as a header to use for the eACL rules check; so
	// every object part in every chain will try to refer to the first part, so caching
	// should help a lot here
	const cachedFirstObjectsNumber = 1000
	fsChain := newFSChainForObjects(placementSvc, c.IsLocalKey, c.networkState, c.cnrSrc, &c.isMaintenance, c.cli)

	aclSvc := v2.New(fsChain,
		v2.WithLogger(c.log),
		v2.WithIRFetcher(newCachedIRFetcher(irFetcher.InnerRingKeys)),
		v2.WithNetmapper(netmapSourceWithNodes{
			Source:         c.netMapSource,
			fsChain:        fsChain,
			netmapContract: c.nCli,
		}),
		v2.WithContainerSource(c.cnrSrc),
		v2.WithTimeProvider(&c.chainTime),
	)
	addNewEpochAsyncNotificationHandler(c, func(event.Event) {
		aclSvc.ResetTokenCheckCache()
	})

	aclChecker := acl.NewChecker(new(acl.CheckerPrm).
		SetEACLSource(c.eaclSrc).
		SetValidator(eacl.NewValidator()).
		SetLocalStorage(ls).
		SetHeaderSource(cachedHeaderSource(sGet, cachedFirstObjectsNumber, c.log)),
	)

	storage := storageForObjectService{
		local:   ls,
		metaSvc: c.metaService,
		putSvc:  sPut,
		keys:    keyStorage,
	}
	server := objectService.New(objSvc, c.cfgObject.pool.search, fsChain, storage, c.metaService, c.key.PrivateKey, c.metricsCollector, aclChecker, aclSvc, coreConstructor, c.log)
	os.server = server

	svcDesc := protoobject.ObjectService_ServiceDesc
	svcDesc.Methods = slices.Clone(protoobject.ObjectService_ServiceDesc.Methods)

	replaceUnaryMethodHandler(&svcDesc, "Head", func(ctx context.Context, req *protoobject.HeadRequest) any {
		return server.HeadBuffered(ctx, req)
	})
	replaceUnaryMethodHandler(&svcDesc, "SearchV2", func(ctx context.Context, req *protoobject.SearchV2Request) any {
		return server.SearchV2Buffered(ctx, req)
	})

	c.cfgGRPC.registerService(func(srv *grpc.Server) {
		srv.RegisterService(&svcDesc, server)
	})
}

func replaceUnaryMethodHandler[REQ any](svcDesc *grpc.ServiceDesc, method string, handler func(context.Context, *REQ) any) {
	mtdInd := slices.IndexFunc(svcDesc.Methods, func(md grpc.MethodDesc) bool { return md.MethodName == method })
	if mtdInd < 0 {
		fatalOnErr(fmt.Errorf("missing %s method handler in object service desc", method))
	}

	svcDesc.Methods[mtdInd].Handler = func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		req := new(REQ)
		if err := dec(req); err != nil {
			return nil, err
		}
		return handler(ctx, req), nil
	}
}

type reputationClientConstructor struct {
	log *zap.Logger

	nmSrc netmapcore.Source

	netState netmapcore.State

	trustStorage *truststorage.Storage

	basicConstructor interface {
		Get(context.Context, netmap.NodeInfo) (clientcore.MultiAddressClient, error)
	}
}

type reputationClient struct {
	clientcore.MultiAddressClient

	peer reputation.PeerID
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

	c.cons.trustStorage.Update(currEpoch, c.peer, sat)
}

func (c *reputationClient) ObjectPutInit(ctx context.Context, hdr object.Object, signer user.Signer, prm client.PrmObjectPutInit) (client.ObjectWriter, error) {
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

func (c *reputationClient) ObjectGetInit(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectGet) (object.Object, *client.PayloadReader, error) {
	hdr, rdr, err := c.MultiAddressClient.ObjectGetInit(ctx, containerID, objectID, signer, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, reading errors are not processed
	c.submitResult(err)

	return hdr, rdr, err
}

func (c *reputationClient) ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHead) (*object.Object, error) {
	res, err := c.MultiAddressClient.ObjectHead(ctx, containerID, objectID, signer, prm)

	c.submitResult(err)

	return res, err
}

func (c *reputationClient) ObjectSearchInit(ctx context.Context, containerID cid.ID, signer user.Signer, prm client.PrmObjectSearch) (*client.ObjectListReader, error) {
	res, err := c.MultiAddressClient.ObjectSearchInit(ctx, containerID, signer, prm)

	// FIXME: (neofs-node#1193) here we submit only initialization errors, reading errors are not processed
	c.submitResult(err)

	return res, err
}

func (c *reputationClientConstructor) Get(ctx context.Context, info netmap.NodeInfo) (clientcore.Client, error) {
	cl, err := c.basicConstructor.Get(ctx, info)
	if err != nil {
		return nil, err
	}

	nm, err := c.nmSrc.NetMap()
	if err == nil {
		key := info.PublicKey()

		nmNodes := nm.Nodes()
		var peer reputation.PeerID

		for i := range nmNodes {
			if bytes.Equal(nmNodes[i].PublicKey(), key) {
				peer.SetPublicKey(nmNodes[i].PublicKey())

				return &reputationClient{
					MultiAddressClient: cl,
					peer:               peer,
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

func cachedHeaderSource(getSvc *getsvc.Service, cacheSize int, l *zap.Logger) headerSource {
	hs := headerSource{
		getsvc: getSvc,
		l:      l.With(zap.String("service", "cached header source"), zap.Int("cache capacity", cacheSize)),
	}

	if cacheSize > 0 {
		var err error
		hs.cache, err = lru.New[oid.Address, *object.Object](cacheSize)
		if err != nil {
			panic(fmt.Errorf("unexpected error in lru.New: %w", err))
		}
	}

	return hs
}

type headerSource struct {
	getsvc *getsvc.Service
	cache  *lru.Cache[oid.Address, *object.Object]
	l      *zap.Logger
}

type headerWriter struct {
	h *object.Object
}

func (h *headerWriter) WriteHeader(o *object.Object) error {
	h.h = o
	return nil
}

func (h headerSource) Head(ctx context.Context, address oid.Address) (*object.Object, error) {
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

	err := h.getsvc.Head(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	h.cache.Add(address, hw.h)

	l.Debug("returning header from network")

	return hw.h, nil
}

type fsChainForObjects struct {
	containercore.Source
	netmapcore.StateDetailed
	placement     *placement.Service
	isLocalPubKey func([]byte) bool
	isMaintenance *atomic.Bool
	*morphClient.Client
}

func newFSChainForObjects(placementSvc *placement.Service, isLocalPubKey func([]byte) bool, ns netmapcore.StateDetailed, cnrSource containercore.Source, isMaintenance *atomic.Bool, fsChainCli *morphClient.Client) *fsChainForObjects {
	return &fsChainForObjects{
		Source:        cnrSource,
		StateDetailed: ns,
		placement:     placementSvc,
		isLocalPubKey: isLocalPubKey,
		isMaintenance: isMaintenance,
		Client:        fsChainCli,
	}
}

// InContainerInLastTwoEpochs checks whether given public key belongs to any SN
// from the referenced container either in the current or the previous NeoFS
// epoch.
//
// Implements [v2.FSChain] interface.
func (x *fsChainForObjects) InContainerInLastTwoEpochs(cnr cid.ID, pub []byte) (bool, error) {
	var inContainer bool
	err := x.placement.ForEachContainerNodePublicKeyInLastTwoEpochs(cnr, func(nodePub []byte) bool {
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
	return x.placement.ForEachContainerNodePublicKeyInLastTwoEpochs(id, f)
}

// SelectContainerNodes implements [objectService.FSChain] interface.
func (x *fsChainForObjects) SelectContainerNodes(cnr cid.ID) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	return x.placement.SelectContainerNodes(cnr)
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
func (x storageForObjectService) SearchObjects(ctx context.Context, cID cid.ID, fs []objectcore.SearchFilter, attrs []string, cursor *objectcore.SearchCursor, count uint16) ([]client.SearchResultItem, []byte, error) {
	return x.local.Search(ctx, cID, fs, attrs, cursor, count)
}

func (x storageForObjectService) VerifyAndStoreObjectLocally(ctx context.Context, obj object.Object) error {
	return x.putSvc.ValidateAndStoreObjectLocally(ctx, obj)
}

func (x storageForObjectService) GetSessionPrivateKey(account user.ID) (ecdsa.PrivateKey, error) {
	k, err := x.keys.GetKey(&account)
	if err != nil {
		return ecdsa.PrivateKey{}, err
	}
	return *k, nil
}

func (x storageForObjectService) GetSessionV2PrivateKey(subjects []sessionv2.Target) (ecdsa.PrivateKey, error) {
	k, err := x.keys.GetKeyBySubjects(subjects)
	if err != nil {
		return ecdsa.PrivateKey{}, err
	}
	return *k, nil
}

type objectSource struct {
	get    *getsvc.Service
	signer neofscrypto.Signer
	server *objectService.Server
}

func (o objectSource) Head(ctx context.Context, addr oid.Address) (*object.Object, error) {
	var hw headerWriter

	var hPrm getsvc.HeadPrm
	hPrm.SetHeaderWriter(&hw)
	hPrm.WithAddress(addr)
	hPrm.WithRawFlag(true)

	err := o.get.Head(ctx, hPrm)

	return hw.h, err
}

func (o objectSource) SearchOne(ctx context.Context, cnr cid.ID, filters object.SearchFilters) (oid.ID, error) {
	var (
		err error
		id  oid.ID
		req = &protoobject.SearchV2Request{
			Body: &protoobject.SearchV2Request_Body{
				ContainerId: cnr.ProtoMessage(),
				Version:     1,
				Filters:     filters.ProtoMessage(),
				Cursor:      "",
				Count:       1,
				Attributes:  nil,
			},
		}
	)

	res, _, err := o.server.ProcessSearch(ctx, req, false, false, cnr)
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
func (c *cfg) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	return c.cfgObject.placement.GetNodesForObject(addr)
}

type netmapSourceWithNodes struct {
	netmapcore.Source
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
		if errors.Is(err, netmap.ErrNotEnoughNodes) {
			return false, nil
		}

		return false, err
	}

	return serverInContainer, nil
}

func (n netmapSourceWithNodes) GetEpochBlock(epoch uint64) (uint32, error) {
	return n.netmapContract.GetEpochBlock(epoch)
}

func (n netmapSourceWithNodes) GetEpochBlockByTime(t uint32) (uint32, error) {
	return n.netmapContract.GetEpochBlockByTime(t)
}

func (c *cfg) GetEpochBlock(epoch uint64) (uint32, error) {
	return c.nCli.GetEpochBlock(epoch)
}

func (c *cfg) GetEpochBlockByTime(t uint32) (uint32, error) {
	return c.nCli.GetEpochBlockByTime(t)
}

// GetContainerNodes reads storage policy of the referenced container from the
// underlying container storage, reads current network map from the underlying
// storage, applies the storage policy to it, gathers storage nodes matching the
// policy and returns sort interface.
//
// GetContainerNodes implements [putsvc.NeoFSNetwork].
func (c *cfg) GetContainerNodes(cnrID cid.ID) (putsvc.ContainerNodes, error) {
	policy, err := c.cfgObject.placement.GetContainerPlacement(cnrID)
	if err != nil {
		return nil, err
	}
	return &containerNodesSorter{
		policy:       policy,
		cnrID:        cnrID,
		placementSvc: c.cfgObject.placement,
	}, nil
}

type cidAndOwner struct {
	cid   cid.ID
	owner user.ID
}

type cachedQuotaState struct {
	takenByUser      uint64
	userQuota        containerClient.Quota
	takenByContainer uint64
	containerQuota   containerClient.Quota
}

func newQuotaTTLcache(size int, ttl time.Duration, netRdr netValueReader[cidAndOwner, cachedQuotaState]) ttlNetCache[cidAndOwner, cachedQuotaState] {
	cache, err := lru.New[cidAndOwner, *valueWithTime[cachedQuotaState]](size)
	fatalOnErr(err)

	return ttlNetCache[cidAndOwner, cachedQuotaState]{
		ttl:     ttl,
		cache:   cache,
		netRdr:  netRdr,
		m:       &sync.RWMutex{},
		progMap: make(map[cidAndOwner]*valueInProgress[cachedQuotaState]),
	}
}

func initQuotas(cnrCli *containerClient.Client, ttl time.Duration) *quotas {
	const quotaCacheLimits = 1024

	return &quotas{
		cache: newQuotaTTLcache(quotaCacheLimits, ttl, func(cIDAndOwner cidAndOwner) (cachedQuotaState, error) {
			cnrQ, err := cnrCli.GetContainerQuota(cIDAndOwner.cid)
			if err != nil {
				return cachedQuotaState{}, fmt.Errorf("fetching container quota: %w", err)
			}
			cnrState, err := cnrCli.GetReportsSummary(cIDAndOwner.cid)
			if err != nil {
				return cachedQuotaState{}, fmt.Errorf("fetching report summary: %w", err)
			}
			userQ, err := cnrCli.GetUserQuota(cIDAndOwner.owner)
			if err != nil {
				return cachedQuotaState{}, fmt.Errorf("fetching user quota: %w", err)
			}
			ownerTakenSpace, err := cnrCli.GetTakenSpaceByUser(cIDAndOwner.owner)
			if err != nil {
				return cachedQuotaState{}, fmt.Errorf("fetching total space taken by user summary: %w", err)
			}
			return cachedQuotaState{
				takenByContainer: cnrState.Size,
				containerQuota:   cnrQ,
				takenByUser:      ownerTakenSpace,
				userQuota:        userQ,
			}, nil
		}),
	}
}

type quotas struct {
	cache ttlNetCache[cidAndOwner, cachedQuotaState]
}

func (q *quotas) AvailableQuotasLeft(cID cid.ID, owner user.ID) (uint64, uint64, error) {
	cachedV, err := q.cache.get(cidAndOwner{cID, owner})
	if err != nil {
		return 0, 0, err
	}

	softLeft := leftLimit(cachedV.containerQuota.SoftLimit, cachedV.takenByContainer, cachedV.userQuota.SoftLimit, cachedV.takenByUser)
	hardLeft := leftLimit(cachedV.containerQuota.HardLimit, cachedV.takenByContainer, cachedV.userQuota.HardLimit, cachedV.takenByUser)

	return softLeft, hardLeft, nil
}

func leftLimit(cnrLimit, cnrTaken, usrLimit, usrTaken uint64) uint64 {
	var cnrLeft uint64
	if cnrLimit == 0 {
		cnrLeft = math.MaxUint64
	} else {
		if cnrTaken < cnrLimit {
			cnrLeft = cnrLimit - cnrTaken
		}
	}
	var usrLeft uint64
	if usrLimit == 0 {
		usrLeft = math.MaxUint64
	} else {
		if usrTaken < usrLimit {
			usrLeft = usrLimit - usrTaken
		}
	}

	return min(cnrLeft, usrLeft)
}

// implements [putsvc.ContainerNodes].
type containerNodesSorter struct {
	policy       placement.Placement
	cnrID        cid.ID
	placementSvc *placement.Service
}

func (x *containerNodesSorter) Unsorted() [][]netmap.NodeInfo { return x.policy.NodeSets }
func (x *containerNodesSorter) PrimaryCounts() []uint         { return x.policy.RepCounts }
func (x *containerNodesSorter) ECRules() []iec.Rule           { return x.policy.ECRules }
func (x *containerNodesSorter) SortForObject(obj oid.ID) ([][]netmap.NodeInfo, error) {
	return x.placementSvc.SortContainerPlacementForObject(x.cnrID, x.policy, obj)
}
