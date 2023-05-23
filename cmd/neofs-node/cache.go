package main

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type netValueReader[K comparable, V any] func(K) (V, error)

type valueWithTime[V any] struct {
	v V
	t time.Time
	// cached error in order to not repeat failed request for some time
	e error
}

// entity that provides TTL cache interface.
type ttlNetCache[K comparable, V any] struct {
	ttl time.Duration

	sz int

	cache *lru.Cache[K, *valueWithTime[V]]

	netRdr netValueReader[K, V]
}

// complicates netValueReader with TTL caching mechanism.
func newNetworkTTLCache[K comparable, V any](sz int, ttl time.Duration, netRdr netValueReader[K, V]) ttlNetCache[K, V] {
	cache, err := lru.New[K, *valueWithTime[V]](sz)
	fatalOnErr(err)

	return ttlNetCache[K, V]{
		ttl:    ttl,
		sz:     sz,
		cache:  cache,
		netRdr: netRdr,
	}
}

// reads value by the key.
//
// updates the value from the network on cache miss or by TTL.
//
// returned value should not be modified.
func (c *ttlNetCache[K, V]) get(key K) (V, error) {
	val, ok := c.cache.Peek(key)
	if ok {
		if time.Since(val.t) < c.ttl {
			return val.v, val.e
		}

		c.cache.Remove(key)
	}

	netVal, err := c.netRdr(key)

	c.set(key, netVal, err)

	return netVal, err
}

func (c *ttlNetCache[K, V]) set(k K, v V, e error) {
	c.cache.Add(k, &valueWithTime[V]{
		v: v,
		t: time.Now(),
		e: e,
	})
}

func (c *ttlNetCache[K, V]) remove(key K) {
	c.cache.Remove(key)
}

// entity that provides LRU cache interface.
type lruNetCache struct {
	cache *lru.Cache[uint64, *netmapSDK.NetMap]

	netRdr netValueReader[uint64, *netmapSDK.NetMap]
}

// newNetworkLRUCache returns wrapper over netValueReader with LRU cache.
func newNetworkLRUCache(sz int, netRdr netValueReader[uint64, *netmapSDK.NetMap]) *lruNetCache {
	cache, err := lru.New[uint64, *netmapSDK.NetMap](sz)
	fatalOnErr(err)

	return &lruNetCache{
		cache:  cache,
		netRdr: netRdr,
	}
}

// reads value by the key.
//
// updates the value from the network on cache miss.
//
// returned value should not be modified.
func (c *lruNetCache) get(key uint64) (*netmapSDK.NetMap, error) {
	val, ok := c.cache.Get(key)
	if ok {
		return val, nil
	}

	val, err := c.netRdr(key)
	if err != nil {
		return nil, err
	}

	c.cache.Add(key, val)

	return val, nil
}

// wrapper over TTL cache of values read from the network
// that implements container storage.
type ttlContainerStorage struct {
	tc ttlNetCache[cid.ID, *container.Container]
}

func newCachedContainerStorage(v container.Source, ttl time.Duration) *ttlContainerStorage {
	const containerCacheSize = 100

	return &ttlContainerStorage{
		tc: newNetworkTTLCache[cid.ID, *container.Container](containerCacheSize, ttl, func(key cid.ID) (*container.Container, error) {
			return v.Get(key)
		}),
	}
}

func (s *ttlContainerStorage) handleRemoval(cnr cid.ID) {
	s.tc.set(cnr, nil, apistatus.ContainerNotFound{})
}

// Get returns container value from the cache. If value is missing in the cache
// or expired, then it returns value from side chain and updates the cache.
func (s *ttlContainerStorage) Get(cnr cid.ID) (*container.Container, error) {
	val, err := s.tc.get(cnr)
	if err != nil {
		return nil, err
	}

	return val, nil
}

type ttlEACLStorage struct {
	tc ttlNetCache[cid.ID, *container.EACL]
}

func newCachedEACLStorage(v container.EACLSource, ttl time.Duration) *ttlEACLStorage {
	const eaclCacheSize = 100

	return &ttlEACLStorage{
		newNetworkTTLCache[cid.ID, *container.EACL](eaclCacheSize, ttl, func(key cid.ID) (*container.EACL, error) {
			return v.GetEACL(key)
		}),
	}
}

// GetEACL returns eACL value from the cache. If value is missing in the cache
// or expired, then it returns value from side chain and updates cache.
func (s *ttlEACLStorage) GetEACL(cnr cid.ID) (*container.EACL, error) {
	val, err := s.tc.get(cnr)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// InvalidateEACL removes cached eACL value.
func (s *ttlEACLStorage) InvalidateEACL(cnr cid.ID) {
	s.tc.remove(cnr)
}

type lruNetmapSource struct {
	netState netmap.State

	cache *lruNetCache
}

func newCachedNetmapStorage(s netmap.State, v netmap.Source) netmap.Source {
	const netmapCacheSize = 10

	return &lruNetmapSource{
		netState: s,
		cache: newNetworkLRUCache(netmapCacheSize, func(key uint64) (*netmapSDK.NetMap, error) {
			return v.GetNetMapByEpoch(key)
		}),
	}
}

func (s *lruNetmapSource) GetNetMap(diff uint64) (*netmapSDK.NetMap, error) {
	return s.getNetMapByEpoch(s.netState.CurrentEpoch() - diff)
}

func (s *lruNetmapSource) GetNetMapByEpoch(epoch uint64) (*netmapSDK.NetMap, error) {
	return s.getNetMapByEpoch(epoch)
}

func (s *lruNetmapSource) getNetMapByEpoch(epoch uint64) (*netmapSDK.NetMap, error) {
	val, err := s.cache.get(epoch)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (s *lruNetmapSource) Epoch() (uint64, error) {
	return s.netState.CurrentEpoch(), nil
}

// wrapper over TTL cache of values read from the network
// that implements container lister.
type ttlContainerLister struct {
	inner  ttlNetCache[string, *cacheItemContainerList]
	client *cntClient.Client
}

// value type for ttlNetCache used by ttlContainerLister.
type cacheItemContainerList struct {
	// protects list from concurrent add/remove ops
	mtx sync.RWMutex
	// actual list of containers owner by the particular user
	list []cid.ID
}

func newCachedContainerLister(c *cntClient.Client, ttl time.Duration) *ttlContainerLister {
	const containerListerCacheSize = 100

	lruCnrListerCache := newNetworkTTLCache(containerListerCacheSize, ttl, func(strID string) (*cacheItemContainerList, error) {
		var id *user.ID

		if strID != "" {
			id = new(user.ID)

			err := id.DecodeString(strID)
			if err != nil {
				return nil, err
			}
		}

		list, err := c.List(id)
		if err != nil {
			return nil, err
		}

		return &cacheItemContainerList{
			list: list,
		}, nil
	})

	return &ttlContainerLister{inner: lruCnrListerCache, client: c}
}

// List returns list of container IDs from the cache. If list is missing in the
// cache or expired, then it returns container IDs from side chain and updates
// the cache.
func (s *ttlContainerLister) List(id *user.ID) ([]cid.ID, error) {
	if id == nil {
		return s.client.List(nil)
	}

	val, err := s.inner.get(id.EncodeToString())
	if err != nil {
		return nil, err
	}

	val.mtx.RLock()
	res := make([]cid.ID, len(val.list))
	copy(res, val.list)
	val.mtx.RUnlock()

	return res, nil
}

// updates cached list of owner's containers: cnr is added if flag is true, otherwise it's removed.
// Concurrent calls can lead to some races:
//   - two parallel additions to missing owner's cache can lead to only one container to be cached
//   - async cache value eviction can lead to idle addition
//
// All described race cases aren't critical since cache values expire anyway, we just try
// to increase cache actuality w/o huge overhead on synchronization.
func (s *ttlContainerLister) update(owner user.ID, cnr cid.ID, add bool) {
	strOwner := owner.EncodeToString()

	vt, ok := s.inner.cache.Peek(strOwner)
	if !ok {
		// we could cache the single cnr but in this case we will disperse
		// with the Sidechain a lot
		return
	}

	if s.inner.ttl <= time.Since(vt.t) {
		return
	}

	item := vt.v

	item.mtx.Lock()
	{
		found := false

		for i := range item.list {
			if found = item.list[i].Equals(cnr); found {
				if !add {
					item.list = append(item.list[:i], item.list[i+1:]...)
					// if list became empty we don't remove the value from the cache
					// since empty list is a correct value, and we don't want to insta
					// re-request it from the Sidechain
				}

				break
			}
		}

		if add && !found {
			item.list = append(item.list, cnr)
		}
	}
	item.mtx.Unlock()
}

type cachedIRFetcher struct {
	tc ttlNetCache[struct{}, [][]byte]
}

func newCachedIRFetcher(f interface{ InnerRingKeys() ([][]byte, error) }) *cachedIRFetcher {
	const (
		irFetcherCacheSize = 1 // we intend to store only one value

		// Without the cache in the testnet we can see several hundred simultaneous
		// requests (neofs-node #1278), so limiting the request rate solves the issue.
		//
		// Exact request rate doesn't really matter because Inner Ring list update
		// happens extremely rare, but there is no side chain events for that as
		// for now (neofs-contract v0.15.0 notary disabled env) to monitor it.
		irFetcherCacheTTL = 30 * time.Second
	)

	irFetcherCache := newNetworkTTLCache(irFetcherCacheSize, irFetcherCacheTTL,
		func(key struct{}) ([][]byte, error) {
			return f.InnerRingKeys()
		},
	)

	return &cachedIRFetcher{tc: irFetcherCache}
}

// InnerRingKeys returns cached list of Inner Ring keys. If keys are missing in
// the cache or expired, then it returns keys from side chain and updates
// the cache.
func (f *cachedIRFetcher) InnerRingKeys() ([][]byte, error) {
	val, err := f.tc.get(struct{}{})
	if err != nil {
		return nil, err
	}

	return val, nil
}

type ttlMaxObjectSizeCache struct {
	mtx         sync.RWMutex
	lastUpdated time.Time
	lastSize    uint64
	src         putsvc.MaxSizeSource
}

func newCachedMaxObjectSizeSource(src putsvc.MaxSizeSource) putsvc.MaxSizeSource {
	return &ttlMaxObjectSizeCache{
		src: src,
	}
}

func (c *ttlMaxObjectSizeCache) MaxObjectSize() uint64 {
	const ttl = time.Second * 30

	c.mtx.RLock()
	prevUpdated := c.lastUpdated
	size := c.lastSize
	c.mtx.RUnlock()

	if time.Since(prevUpdated) < ttl {
		return size
	}

	c.mtx.Lock()
	size = c.lastSize
	if !c.lastUpdated.After(prevUpdated) {
		size = c.src.MaxObjectSize()
		c.lastSize = size
		c.lastUpdated = time.Now()
	}
	c.mtx.Unlock()

	return size
}
