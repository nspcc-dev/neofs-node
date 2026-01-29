package nns

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// FSChain provides base non-contract functionality of the FS chain required for
// NNS name resolution.
type FSChain interface {
	HasUserInNNS(name string, addr util.Uint160) (bool, error)
}

// Resolver is an NNS name resolver.
type Resolver struct {
	fsChain  FSChain
	nnsCache *lru.Cache[string, bool]
}

// NewResolver creates a new NNS Resolver with given FSChain.
func NewResolver(fs FSChain) *Resolver {
	cache, err := lru.New[string, bool](1000)
	if err != nil {
		panic(fmt.Errorf("unexpected error in lru.New for nns resolver: %w", err))
	}
	return &Resolver{fsChain: fs, nnsCache: cache}
}

// HasUser checks whether the user with given userID is registered
// in NNS under the specified name.
func (r Resolver) HasUser(name string, userID user.ID) (bool, error) {
	cacheKey := name + string(userID[:])
	hasUser, ok := r.nnsCache.Get(cacheKey)
	if ok {
		return hasUser, nil
	}
	hasUser, err := r.fsChain.HasUserInNNS(name, userID.ScriptHash())
	if err != nil {
		return false, err
	}
	r.nnsCache.Add(cacheKey, hasUser)
	return hasUser, nil
}

// PurgeCache clears the internal cache of resolved names.
func (r Resolver) PurgeCache() {
	r.nnsCache.Purge()
}
