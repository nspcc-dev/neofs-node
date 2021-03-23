package main

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	netmapSDK "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

type netValueReader func(interface{}) (interface{}, error)

type valueWithTime struct {
	v interface{}
	t time.Time
}

// entity that provides TTL cache interface.
type ttlNetCache struct {
	mtx sync.Mutex

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
	c.mtx.Lock()
	defer c.mtx.Unlock()

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

// entity that provides LRU cache interface.
type lruNetCache struct {
	mtx sync.Mutex

	cache *lru.Cache

	netRdr netValueReader
}

// complicates netValueReader with LRU caching mechanism.
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
	c.mtx.Lock()
	defer c.mtx.Unlock()

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

func newCachedContainerStorage(v container.Source) container.Source {
	const (
		containerCacheSize = 100
		containerCacheTTL  = 30 * time.Second
	)

	lruCnrCache := newNetworkTTLCache(containerCacheSize, containerCacheTTL, func(key interface{}) (interface{}, error) {
		cid := containerSDK.NewID()

		err := cid.Parse(key.(string))
		if err != nil {
			return nil, err
		}

		return v.Get(cid)
	})

	return (*ttlContainerStorage)(lruCnrCache)
}

func (s *ttlContainerStorage) Get(cid *containerSDK.ID) (*containerSDK.Container, error) {
	val, err := (*ttlNetCache)(s).get(cid.String())
	if err != nil {
		return nil, err
	}

	return val.(*containerSDK.Container), nil
}

type ttlEACLStorage ttlNetCache

func newCachedEACLStorage(v eacl.Storage) eacl.Storage {
	const (
		eaclCacheSize = 100
		eaclCacheTTL  = 30 * time.Second
	)

	lruCnrCache := newNetworkTTLCache(eaclCacheSize, eaclCacheTTL, func(key interface{}) (interface{}, error) {
		cid := containerSDK.NewID()

		err := cid.Parse(key.(string))
		if err != nil {
			return nil, err
		}

		return v.GetEACL(cid)
	})

	return (*ttlEACLStorage)(lruCnrCache)
}

func (s *ttlEACLStorage) GetEACL(cid *containerSDK.ID) (*eaclSDK.Table, error) {
	val, err := (*ttlNetCache)(s).get(cid.String())
	if err != nil {
		return nil, err
	}

	return val.(*eaclSDK.Table), nil
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
