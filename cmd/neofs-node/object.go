package main

import (
	"context"
	"errors"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	deletesvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/delete/v2"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	getsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/get/v2"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	headsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/head/v2"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	putsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/put/v2"
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	rangesvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/range/v2"
	rangehashsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/rangehash"
	rangehashsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/rangehash/v2"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	searchsvcV2 "github.com/nspcc-dev/neofs-node/pkg/services/object/search/v2"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/panjf2000/ants/v2"
)

type objectSvc struct {
	put *putsvcV2.Service

	search *searchsvcV2.Service

	head *headsvcV2.Service

	get *getsvcV2.Service

	rng *rangesvcV2.Service

	rngHash *rangehashsvcV2.Service

	delete *deletesvcV2.Service
}

type inMemBucket struct {
	bucket.Bucket
	*sync.RWMutex
	items map[string][]byte
}

type maxSzSrc struct {
	v uint64
}

func (s *maxSzSrc) MaxObjectSize() uint64 {
	return s.v
}

func newBucket() bucket.Bucket {
	return &inMemBucket{
		RWMutex: new(sync.RWMutex),
		items:   map[string][]byte{},
	}
}

func (b *inMemBucket) Get(key []byte) ([]byte, error) {
	b.RLock()
	v, ok := b.items[base58.Encode(key)]
	b.RUnlock()

	if !ok {
		return nil, errors.New("not found")
	}

	return v, nil
}

func (b *inMemBucket) Set(key, value []byte) error {
	k := base58.Encode(key)

	b.Lock()
	b.items[k] = makeCopy(value)
	b.Unlock()

	return nil
}

func (b *inMemBucket) Iterate(handler bucket.FilterHandler) error {
	if handler == nil {
		return bucket.ErrNilFilterHandler
	}

	b.RLock()
	for key, val := range b.items {
		k, err := base58.Decode(key)
		if err != nil {
			panic(err)
		}

		v := makeCopy(val)

		if !handler(k, v) {
			return bucket.ErrIteratingAborted
		}
	}
	b.RUnlock()

	return nil
}

func makeCopy(val []byte) []byte {
	tmp := make([]byte, len(val))
	copy(tmp, val)

	return tmp
}

func (s *objectSvc) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	return s.put.Put(ctx)
}

func (s *objectSvc) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	return s.head.Head(ctx, req)
}

func (s *objectSvc) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	return s.search.Search(ctx, req)
}

func (s *objectSvc) Get(ctx context.Context, req *object.GetRequest) (object.GetObjectStreamer, error) {
	return s.get.Get(ctx, req)
}

func (s *objectSvc) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	return s.delete.Delete(ctx, req)
}

func (s *objectSvc) GetRange(ctx context.Context, req *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	return s.rng.GetRange(ctx, req)
}

func (s *objectSvc) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return s.rngHash.GetRangeHash(ctx, req)
}

func initObjectService(c *cfg) {
	ls := localstore.New(
		c.cfgObject.blobstorage,
		c.cfgObject.metastorage,
	)
	keyStorage := util.NewKeyStorage(c.key, c.privateTokenStore)
	nodeOwner := owner.NewID()

	neo3Wallet, err := owner.NEO3WalletFromPublicKey(&c.key.PublicKey)
	fatalOnErr(err)

	nodeOwner.SetNeo3Wallet(neo3Wallet)

	sPut := putsvc.NewService(
		putsvc.WithKeyStorage(keyStorage),
		putsvc.WithMaxSizeSource(&maxSzSrc{c.cfgObject.maxObjectSize}),
		putsvc.WithLocalStorage(ls),
		putsvc.WithContainerSource(c.cfgObject.cnrStorage),
		putsvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		putsvc.WithLocalAddressSource(c),
	)

	sPutV2 := putsvcV2.NewService(
		putsvcV2.WithInternalService(sPut),
	)

	sSearch := searchsvc.NewService(
		searchsvc.WithKeyStorage(keyStorage),
		searchsvc.WithLocalStorage(ls),
		searchsvc.WithContainerSource(c.cfgObject.cnrStorage),
		searchsvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		searchsvc.WithLocalAddressSource(c),
	)

	sSearchV2 := searchsvcV2.NewService(
		searchsvcV2.WithInternalService(sSearch),
	)

	sHead := headsvc.NewService(
		headsvc.WithKeyStorage(keyStorage),
		headsvc.WithLocalStorage(ls),
		headsvc.WithContainerSource(c.cfgObject.cnrStorage),
		headsvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		headsvc.WithLocalAddressSource(c),
		headsvc.WithRightChildSearcher(searchsvc.NewRightChildSearcher(sSearch)),
	)

	sHeadV2 := headsvcV2.NewService(
		headsvcV2.WithInternalService(sHead),
	)

	pool, err := ants.NewPool(10)
	fatalOnErr(err)

	sRange := rangesvc.NewService(
		rangesvc.WithKeyStorage(keyStorage),
		rangesvc.WithLocalStorage(ls),
		rangesvc.WithContainerSource(c.cfgObject.cnrStorage),
		rangesvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		rangesvc.WithLocalAddressSource(c),
		rangesvc.WithWorkerPool(pool),
		rangesvc.WithHeadService(sHead),
	)

	sRangeV2 := rangesvcV2.NewService(
		rangesvcV2.WithInternalService(sRange),
	)

	sGet := getsvc.NewService(
		getsvc.WithRangeService(sRange),
	)

	sGetV2 := getsvcV2.NewService(
		getsvcV2.WithInternalService(sGet),
	)

	sRangeHash := rangehashsvc.NewService(
		rangehashsvc.WithKeyStorage(keyStorage),
		rangehashsvc.WithLocalStorage(ls),
		rangehashsvc.WithContainerSource(c.cfgObject.cnrStorage),
		rangehashsvc.WithNetworkMapSource(c.cfgObject.netMapStorage),
		rangehashsvc.WithLocalAddressSource(c),
		rangehashsvc.WithHeadService(sHead),
		rangehashsvc.WithRangeService(sRange),
	)

	sRangeHashV2 := rangehashsvcV2.NewService(
		rangehashsvcV2.WithInternalService(sRangeHash),
	)

	sDelete := deletesvc.NewService(
		deletesvc.WithKeyStorage(keyStorage),
		deletesvc.WitHeadService(sHead),
		deletesvc.WithPutService(sPut),
		deletesvc.WithOwnerID(nodeOwner),
		deletesvc.WithLinkingHeader(
			headsvc.NewRelationHeader(searchsvc.NewLinkingSearcher(sSearch), sHead),
		),
	)

	sDeleteV2 := deletesvcV2.NewService(
		deletesvcV2.WithInternalService(sDelete),
	)

	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(
			acl.New(
				acl.WithSenderClassifier(
					acl.NewSenderClassifier(
						c.cfgNetmap.wrapper,
						c.cfgNetmap.wrapper,
					),
				),
				acl.WithContainerSource(
					c.cfgObject.cnrStorage,
				),
				acl.WithNextService(
					objectService.NewSignService(
						c.key,
						objectService.NewTransportSplitter(
							c.cfgGRPC.maxChunkSize,
							c.cfgGRPC.maxAddrAmount,
							&objectSvc{
								put:     sPutV2,
								search:  sSearchV2,
								head:    sHeadV2,
								rng:     sRangeV2,
								get:     sGetV2,
								rngHash: sRangeHashV2,
								delete:  sDeleteV2,
							},
						),
					),
				),
				acl.WithLocalStorage(ls),
				acl.WithEACLValidatorOptions(
					eacl.WithMorphClient(c.cfgObject.cnrClient),
					eacl.WithLogger(c.log),
				),
			),
		),
	)
}
