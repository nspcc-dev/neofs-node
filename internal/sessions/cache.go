package sessions

import (
	"crypto/sha256"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
)

// NewObjectSessionsCache creates ObjectSessionsCache with a provided size.
// Caches for v1 and v2 tokens are independent. Size must be positive.
func NewObjectSessionsCache(cacheSize int) *ObjectSessionsCache {
	cache, err := lru.New[[sha256.Size]byte, results](cacheSize)
	if err != nil {
		panic(err)
	}
	return &ObjectSessionsCache{cache: cache}
}

type results struct {
	tV1 session.Object
	tV2 sessionv2.Token
	err error
}

// ObjectSessionsCache caches object session tokens based on their hashes, both
// v1 and v2 versions. Caches are LRU, there is no garanty a put token will be
// available later.
type ObjectSessionsCache struct {
	cache *lru.Cache[[sha256.Size]byte, results]
}

// AuthenticateTokenV1 checks whether v1 token was cached as a correctly signed one.
// On cache miss, authOnMiss is called and its result is saved for future calls
// with the same tokenKey. tokenKey must be the same for the same token for cache
// to work properly.
func (ch *ObjectSessionsCache) AuthenticateTokenV1(tokenKey [sha256.Size]byte, authOnMiss func() (session.Object, error)) (session.Object, error) {
	res, ok := ch.cache.Get(tokenKey)
	if !ok {
		res.tV1, res.err = authOnMiss()
		ch.cache.Add(tokenKey, res)
	}
	return res.tV1, res.err
}

// AuthenticateTokenV2 works the same as [ObjectSessionsCache.AuthenticateTokenV1]
// but for v2 session tokens.
func (ch *ObjectSessionsCache) AuthenticateTokenV2(tokenKey [sha256.Size]byte, authOnMiss func() (sessionv2.Token, error)) (sessionv2.Token, error) {
	res, ok := ch.cache.Get(tokenKey)
	if !ok {
		res.tV2, res.err = authOnMiss()
		ch.cache.Add(tokenKey, res)
	}
	return res.tV2, res.err
}

// ResetCache resets all cached tokens.
func (ch *ObjectSessionsCache) ResetCache() {
	ch.cache.Purge()
}
