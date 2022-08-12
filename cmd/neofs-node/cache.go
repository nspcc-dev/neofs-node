package main

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type netValueReader func(interface{}) (interface{}, error)

type valueWithTime struct {
	v interface{}
	t time.Time
	// cached error in order to not repeat failed request for some time
	e error
}

// entity that provides TTL cache interface.
type ttlNetCache struct {
	ttl time.Duration

	sz int

	cache *lru.Cache

	netRdr netValueReader
}

// complicates netValueReader with TTL caching mechanism.
func newNetworkTTLCache(sz int, ttl time.Duration, netRdr netValueReader) *ttlNetCache {
	cache, err := lru.New(sz)
	fatalOnErr(err)

	return &ttlNetCache{
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
func (c *ttlNetCache) get(key interface{}) (interface{}, error) {
	val, ok := c.cache.Peek(key)
	if ok {
		valWithTime := val.(*valueWithTime)

		if time.Since(valWithTime.t) < c.ttl {
			return valWithTime.v, valWithTime.e
		}

		c.cache.Remove(key)
	}

	val, err := c.netRdr(key)

	c.set(key, val, err)

	return val, err
}

func (c *ttlNetCache) set(k, v interface{}, e error) {
	c.cache.Add(k, &valueWithTime{
		v: v,
		t: time.Now(),
		e: e,
	})
}

func (c *ttlNetCache) remove(key interface{}) {
	c.cache.Remove(key)
}

// entity that provides LRU cache interface.
type lruNetCache struct {
	cache *lru.Cache

	netRdr netValueReader
}

// newNetworkLRUCache returns wrapper over netValueReader with LRU cache.
func newNetworkLRUCache(sz int, netRdr netValueReader) *lruNetCache {
	cache, err := lru.New(sz)
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
func (c *lruNetCache) get(key interface{}) (interface{}, error) {
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
type ttlContainerStorage ttlNetCache

func newCachedContainerStorage(v container.Source) *ttlContainerStorage {
	const (
		containerCacheSize = 100
		containerCacheTTL  = 30 * time.Second
	)

	lruCnrCache := newNetworkTTLCache(containerCacheSize, containerCacheTTL, func(key interface{}) (interface{}, error) {
		var id cid.ID

		err := id.DecodeString(key.(string))
		if err != nil {
			return nil, err
		}

		return v.Get(id)
	})

	return (*ttlContainerStorage)(lruCnrCache)
}

func (s *ttlContainerStorage) handleRemoval(cnr cid.ID) {
	(*ttlNetCache)(s).set(cnr.EncodeToString(), nil, apistatus.ContainerNotFound{})
}

// Get returns container value from the cache. If value is missing in the cache
// or expired, then it returns value from side chain and updates the cache.
func (s *ttlContainerStorage) Get(cnr cid.ID) (*container.Container, error) {
	val, err := (*ttlNetCache)(s).get(cnr.EncodeToString())
	if err != nil {
		return nil, err
	}

	return val.(*container.Container), nil
}

type ttlEACLStorage ttlNetCache

func newCachedEACLStorage(v eacl.Source) *ttlEACLStorage {
	const (
		eaclCacheSize = 100
		eaclCacheTTL  = 30 * time.Second
	)

	lruCnrCache := newNetworkTTLCache(eaclCacheSize, eaclCacheTTL, func(key interface{}) (interface{}, error) {
		var id cid.ID

		err := id.DecodeString(key.(string))
		if err != nil {
			return nil, err
		}

		return v.GetEACL(id)
	})

	return (*ttlEACLStorage)(lruCnrCache)
}

// GetEACL returns eACL value from the cache. If value is missing in the cache
// or expired, then it returns value from side chain and updates cache.
func (s *ttlEACLStorage) GetEACL(cnr cid.ID) (*container.EACL, error) {
	val, err := (*ttlNetCache)(s).get(cnr.EncodeToString())
	if err != nil {
		return nil, err
	}

	return val.(*container.EACL), nil
}

// InvalidateEACL removes cached eACL value.
func (s *ttlEACLStorage) InvalidateEACL(cnr cid.ID) {
	(*ttlNetCache)(s).remove(cnr.EncodeToString())
}

type lruNetmapSource struct {
	netState netmap.State

	cache *lruNetCache
}

func newCachedNetmapStorage(s netmap.State, v netmap.Source) netmap.Source {
	const netmapCacheSize = 10

	lruNetmapCache := newNetworkLRUCache(netmapCacheSize, func(key interface{}) (interface{}, error) {
		return v.GetNetMapByEpoch(key.(uint64))
	})

	return &lruNetmapSource{
		netState: s,
		cache:    lruNetmapCache,
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

	return val.(*netmapSDK.NetMap), nil
}

func (s *lruNetmapSource) Epoch() (uint64, error) {
	return s.netState.CurrentEpoch(), nil
}

// wrapper over TTL cache of values read from the network
// that implements container lister.
type ttlContainerLister ttlNetCache

// value type for ttlNetCache used by ttlContainerLister.
type cacheItemContainerList struct {
	// protects list from concurrent add/remove ops
	mtx sync.RWMutex
	// actual list of containers owner by the particular user
	list []cid.ID
}

func newCachedContainerLister(c *cntClient.Client) *ttlContainerLister {
	const (
		containerListerCacheSize = 100
		containerListerCacheTTL  = 30 * time.Second
	)

	lruCnrListerCache := newNetworkTTLCache(containerListerCacheSize, containerListerCacheTTL, func(key interface{}) (interface{}, error) {
		var (
			id    *user.ID
			strID = key.(string)
		)

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

	return (*ttlContainerLister)(lruCnrListerCache)
}

// List returns list of container IDs from the cache. If list is missing in the
// cache or expired, then it returns container IDs from side chain and updates
// the cache.
func (s *ttlContainerLister) List(id *user.ID) ([]cid.ID, error) {
	var str string

	if id != nil {
		str = id.EncodeToString()
	}

	val, err := (*ttlNetCache)(s).get(str)
	if err != nil {
		return nil, err
	}

	// panic on typecast below is OK since developer must be careful,
	// runtime can do nothing with wrong type occurrence
	item := val.(*cacheItemContainerList)

	item.mtx.RLock()
	res := make([]cid.ID, len(item.list))
	copy(res, item.list)
	item.mtx.RUnlock()

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

	val, ok := (*ttlNetCache)(s).cache.Get(strOwner)
	if !ok {
		if add {
			// first cached owner's container
			(*ttlNetCache)(s).set(strOwner, &cacheItemContainerList{
				list: []cid.ID{cnr},
			}, nil)
		}

		// no-op on removal when no owner's containers are cached

		return
	}

	// panic on typecast below is OK since developer must be careful,
	// runtime can do nothing with wrong type occurrence
	item := val.(*valueWithTime).v.(*cacheItemContainerList)

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

type cachedIRFetcher ttlNetCache

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
		func(key interface{}) (interface{}, error) {
			return f.InnerRingKeys()
		},
	)

	return (*cachedIRFetcher)(irFetcherCache)
}

// InnerRingKeys returns cached list of Inner Ring keys. If keys are missing in
// the cache or expired, then it returns keys from side chain and updates
// the cache.
func (f *cachedIRFetcher) InnerRingKeys() ([][]byte, error) {
	val, err := (*ttlNetCache)(f).get("")
	if err != nil {
		return nil, err
	}

	return val.([][]byte), nil
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
