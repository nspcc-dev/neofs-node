package tree

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type clientCache struct {
	sync.Mutex
	simplelru.LRU
}

const (
	defaultClientCacheSize      = 10
	defaultClientConnectTimeout = time.Second * 2
)

func (c *clientCache) init() {
	l, _ := simplelru.NewLRU(defaultClientCacheSize, func(key, value interface{}) {
		_ = value.(*grpc.ClientConn).Close()
	})
	c.LRU = *l
}

func (c *clientCache) get(ctx context.Context, netmapAddr string) (TreeServiceClient, error) {
	c.Lock()
	ccInt, ok := c.LRU.Get(netmapAddr)
	c.Unlock()

	if ok {
		cc := ccInt.(*grpc.ClientConn)
		if s := cc.GetState(); s == connectivity.Idle || s == connectivity.Ready {
			return NewTreeServiceClient(cc), nil
		}
		_ = cc.Close()
	}

	cc, err := dialTreeService(ctx, netmapAddr)
	if err != nil {
		return nil, err
	}

	c.Lock()
	c.LRU.Add(netmapAddr, cc)
	c.Unlock()

	return NewTreeServiceClient(cc), nil
}

func dialTreeService(ctx context.Context, netmapAddr string) (*grpc.ClientConn, error) {
	var netAddr network.Address
	if err := netAddr.FromString(netmapAddr); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, defaultClientConnectTimeout)
	cc, err := grpc.DialContext(ctx, netAddr.URIAddr(),
		grpc.WithInsecure(),
		grpc.WithBlock())
	cancel()

	return cc, err
}
