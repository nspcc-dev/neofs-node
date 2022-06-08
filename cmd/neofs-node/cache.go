package main

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type netValueReader func(interface{}) (interface{}, error)

type valueWithTime struct {
	v interface{}
	t time.Time
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
			return valWithTime.v, nil
		}

		c.cache.Remove(key)
	}

	val, err := c.netRdr(key)
	if err != nil {
		return nil, err
	}

	c.cache.Add(key, &valueWithTime{
		v: val,
		t: time.Now(),
	})

	return val, nil
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

// Get returns container value from the cache. If value is missing in the cache
// or expired, then it returns value from side chain and updates the cache.
func (s *ttlContainerStorage) Get(cnr cid.ID) (*containerSDK.Container, error) {
	val, err := (*ttlNetCache)(s).get(cnr.EncodeToString())
	if err != nil {
		return nil, err
	}

	return val.(*containerSDK.Container), nil
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
func (s *ttlEACLStorage) GetEACL(cnr cid.ID) (*eaclSDK.Table, error) {
	val, err := (*ttlNetCache)(s).get(cnr.EncodeToString())
	if err != nil {
		return nil, err
	}

	return val.(*eaclSDK.Table), nil
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

func (s *lruNetmapSource) GetNetMap(diff uint64) (*netmapSDK.Netmap, error) {
	return s.getNetMapByEpoch(s.netState.CurrentEpoch() - diff)
}

func (s *lruNetmapSource) GetNetMapByEpoch(epoch uint64) (*netmapSDK.Netmap, error) {
	return s.getNetMapByEpoch(epoch)
}

func (s *lruNetmapSource) getNetMapByEpoch(epoch uint64) (*netmapSDK.Netmap, error) {
	val, err := s.cache.get(epoch)
	if err != nil {
		return nil, err
	}

	return val.(*netmapSDK.Netmap), nil
}

func (s *lruNetmapSource) Epoch() (uint64, error) {
	return s.netState.CurrentEpoch(), nil
}

// wrapper over TTL cache of values read from the network
// that implements container lister.
type ttlContainerLister ttlNetCache

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

		return c.List(id)
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

	return val.([]cid.ID), nil
}

// InvalidateContainerList removes cached list of container IDs.
func (s *ttlContainerLister) InvalidateContainerList(id user.ID) {
	(*ttlNetCache)(s).remove(id.EncodeToString())
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
