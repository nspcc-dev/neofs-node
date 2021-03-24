package keycache

import (
	"crypto/ecdsa"

	"github.com/hashicorp/golang-lru/simplelru"
	crypto "github.com/nspcc-dev/neofs-crypto"
)

// KeyCache represents public key cache.
type KeyCache interface {
	UnmarshalPublic([]byte) *ecdsa.PublicKey
}

type keyCache struct {
	simplelru.LRU
}

// New creates new key cache.
func New(capacity int) KeyCache {
	c, err := simplelru.NewLRU(capacity, nil)
	if err != nil {
		// panic occurs only with nil capacity, which is a programmer error.
		panic(err)
	}
	return &keyCache{
		LRU: *c,
	}
}

// UnmarshalPublic implements KeyCache interface.
func (k *keyCache) UnmarshalPublic(data []byte) *ecdsa.PublicKey {
	s := string(data)
	if pub, ok := k.Get(s); ok {
		return pub.(*ecdsa.PublicKey)
	}

	pub := crypto.UnmarshalPublicKey(data)
	k.Add(s, pub)
	return pub
}

var global = New(100)

func UnmarshalPublicKey(data []byte) *ecdsa.PublicKey {
	return global.UnmarshalPublic(data)
}
