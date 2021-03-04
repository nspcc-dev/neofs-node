package main

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
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
			valWithTime.t = time.Now()
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
