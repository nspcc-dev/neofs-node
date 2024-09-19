package tree

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type clientCache struct {
	sync.Mutex
	simplelru.LRU[string, cacheItem]
}

type cacheItem struct {
	cc      *grpc.ClientConn
	lastTry time.Time
}

const (
	defaultClientCacheSize   = 10
	defaultReconnectInterval = time.Second * 15
)

var errRecentlyFailed = errors.New("client has recently failed")

func (c *clientCache) init() {
	l, _ := simplelru.NewLRU[string, cacheItem](defaultClientCacheSize, func(_ string, v cacheItem) {
		if conn := v.cc; conn != nil {
			_ = conn.Close()
		}
	})
	c.LRU = *l
}

func (c *clientCache) get(netmapAddr string) (TreeServiceClient, error) {
	c.Lock()
	ccInt, ok := c.LRU.Get(netmapAddr)
	c.Unlock()

	if ok {
		if ccInt.cc == nil {
			if d := time.Since(ccInt.lastTry); d < defaultReconnectInterval {
				return nil, fmt.Errorf("%w: %s till the next reconnection to %s",
					errRecentlyFailed, d, netmapAddr)
			}
		} else {
			if s := ccInt.cc.GetState(); s == connectivity.Idle || s == connectivity.Ready {
				return NewTreeServiceClient(ccInt.cc), nil
			}
			_ = ccInt.cc.Close()
		}
	}

	cc, err := dialTreeService(netmapAddr)
	lastTry := time.Now()

	c.Lock()
	if err != nil {
		c.LRU.Add(netmapAddr, cacheItem{cc: nil, lastTry: lastTry})
	} else {
		c.LRU.Add(netmapAddr, cacheItem{cc: cc, lastTry: lastTry})
	}
	c.Unlock()

	if err != nil {
		return nil, err
	}

	return NewTreeServiceClient(cc), nil
}

func dialTreeService(netmapAddr string) (*grpc.ClientConn, error) {
	var netAddr network.Address
	if err := netAddr.FromString(netmapAddr); err != nil {
		return nil, err
	}

	opts := make([]grpc.DialOption, 0, 1)

	// FIXME(@fyrchik): ugly hack #1322
	if !strings.HasPrefix(netAddr.URIAddr(), "grpcs:") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	return grpc.NewClient(netAddr.URIAddr(), opts...)
}
