package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type singleClient struct {
	sync.RWMutex
	client      clientcore.Client
	lastAttempt time.Time
}

type multiClient struct {
	mtx sync.RWMutex

	clients map[string]*singleClient

	// addrMtx protects addr field. Should not be taken before the mtx.
	addrMtx sync.RWMutex
	addr    network.AddressGroup

	opts ClientCacheOpts
}

const defaultReconnectInterval = time.Second * 30

func newMultiClient(addr network.AddressGroup, opts ClientCacheOpts) *multiClient {
	if opts.ReconnectTimeout <= 0 {
		opts.ReconnectTimeout = defaultReconnectInterval
	}
	return &multiClient{
		clients: make(map[string]*singleClient),
		addr:    addr,
		opts:    opts,
	}
}

func (x *multiClient) createForAddress(addr network.Address) (clientcore.Client, error) {
	var (
		c       client.Client
		prmInit client.PrmInit
		prmDial client.PrmDial
	)

	prmDial.SetServerURI(addr.URIAddr())

	if x.opts.Key != nil {
		prmInit.SetDefaultSigner(neofsecdsa.SignerRFC6979(*x.opts.Key))
	}

	if x.opts.DialTimeout > 0 {
		prmDial.SetTimeout(x.opts.DialTimeout)
	}

	if x.opts.StreamTimeout > 0 {
		prmDial.SetStreamTimeout(x.opts.StreamTimeout)
	}

	if x.opts.ResponseCallback != nil {
		prmInit.SetResponseInfoCallback(x.opts.ResponseCallback)
	}

	c.Init(prmInit)
	err := c.Dial(prmDial)
	if err != nil {
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}

	return &c, nil
}

// updateGroup replaces current multiClient addresses with a new group.
// Old addresses not present in group are removed.
func (x *multiClient) updateGroup(group network.AddressGroup) {
	// Firstly, remove old clients.
	cache := make([]string, 0, group.Len())
	group.IterateAddresses(func(a network.Address) bool {
		cache = append(cache, a.String())
		return false
	})

	x.addrMtx.RLock()
	oldGroup := x.addr
	x.addrMtx.RUnlock()
	if len(oldGroup) == len(cache) {
		needUpdate := false
		for i := range oldGroup {
			if cache[i] != oldGroup[i].String() {
				needUpdate = true
				break
			}
		}
		if !needUpdate {
			return
		}
	}

	x.mtx.Lock()
	defer x.mtx.Unlock()
loop:
	for a := range x.clients {
		for i := range cache {
			if cache[i] == a {
				continue loop
			}
		}
		delete(x.clients, a)
	}

	// Then add new clients.
	x.addrMtx.Lock()
	x.addr = group
	x.addrMtx.Unlock()
}

var errRecentlyFailed = errors.New("client has recently failed, skipping")

func (x *multiClient) iterateClients(ctx context.Context, f func(clientcore.Client) error) error {
	var firstErr error

	x.addrMtx.RLock()
	group := x.addr
	x.addrMtx.RUnlock()

	group.IterateAddresses(func(addr network.Address) bool {
		select {
		case <-ctx.Done():
			firstErr = context.Canceled
			return true
		default:
		}

		var err error

		c, err := x.client(addr)
		if err == nil {
			err = f(c)
		}

		// non-status logic error that could be returned
		// from the SDK client; should not be considered
		// as a connection error
		var siErr *objectSDK.SplitInfoError

		success := err == nil || errors.Is(err, context.Canceled) || errors.As(err, &siErr)
		if success || firstErr == nil || errors.Is(firstErr, errRecentlyFailed) {
			firstErr = err
		}

		if err != nil {
			x.ReportError(err)
		}

		return success
	})

	return firstErr
}

func (x *multiClient) ReportError(err error) {
	if errors.Is(err, errRecentlyFailed) {
		return
	}

	if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
		return
	}

	// non-status logic error that could be returned
	// from the SDK client; should not be considered
	// as a connection error
	var siErr *objectSDK.SplitInfoError
	if errors.As(err, &siErr) {
		return
	}

	// Dropping all clients here is not necessary, we do this
	// because `multiClient` doesn't yet provide convenient interface
	// for reporting individual errors for streaming operations.
	x.mtx.RLock()
	for _, sc := range x.clients {
		sc.invalidate()
	}
	x.mtx.RUnlock()
}

func (s *singleClient) invalidate() {
	s.Lock()
	if s.client != nil {
		_ = s.client.Close()
	}
	s.client = nil
	s.Unlock()
}

func (x *multiClient) ObjectPutInit(ctx context.Context, p client.PrmObjectPutInit) (res *client.ObjectWriter, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectPutInit(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ContainerAnnounceUsedSpace(ctx context.Context, prm client.PrmAnnounceSpace) (res *client.ResAnnounceSpace, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ContainerAnnounceUsedSpace(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectDelete(ctx context.Context, p client.PrmObjectDelete) (res *client.ResObjectDelete, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectDelete(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ObjectGetInit(ctx context.Context, p client.PrmObjectGet) (res *client.ObjectReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectGetInit(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ObjectRangeInit(ctx context.Context, p client.PrmObjectRange) (res *client.ObjectRangeReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectRangeInit(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ObjectHead(ctx context.Context, p client.PrmObjectHead) (res *client.ResObjectHead, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectHead(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ObjectHash(ctx context.Context, p client.PrmObjectHash) (res *client.ResObjectHash, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectHash(ctx, p)
		return err
	})

	return
}

func (x *multiClient) ObjectSearchInit(ctx context.Context, p client.PrmObjectSearch) (res *client.ObjectListReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectSearchInit(ctx, p)
		return err
	})

	return
}

func (x *multiClient) AnnounceLocalTrust(ctx context.Context, prm client.PrmAnnounceLocalTrust) (res *client.ResAnnounceLocalTrust, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.AnnounceLocalTrust(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) AnnounceIntermediateTrust(ctx context.Context, prm client.PrmAnnounceIntermediateTrust) (res *client.ResAnnounceIntermediateTrust, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.AnnounceIntermediateTrust(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) ExecRaw(f func(client *rawclient.Client) error) error {
	panic("multiClient.ExecRaw() must not be called")
}

func (x *multiClient) Close() error {
	x.mtx.RLock()

	{
		for _, c := range x.clients {
			if c.client != nil {
				_ = c.client.Close()
			}
		}
	}

	x.mtx.RUnlock()

	return nil
}

func (x *multiClient) RawForAddress(addr network.Address, f func(client *rawclient.Client) error) error {
	c, err := x.client(addr)
	if err != nil {
		return err
	}

	err = c.ExecRaw(f)
	if err != nil {
		x.ReportError(err)
	}
	return err
}

func (x *multiClient) client(addr network.Address) (clientcore.Client, error) {
	strAddr := addr.String()

	x.mtx.RLock()
	c, cached := x.clients[strAddr]
	x.mtx.RUnlock()

	if cached {
		c.RLock()
		if c.client != nil {
			cl := c.client
			c.RUnlock()
			return cl, nil
		}
		if x.opts.ReconnectTimeout != 0 && time.Since(c.lastAttempt) < x.opts.ReconnectTimeout {
			c.RUnlock()
			return nil, errRecentlyFailed
		}
		c.RUnlock()
	} else {
		var ok bool
		x.mtx.Lock()
		c, ok = x.clients[strAddr]
		if !ok {
			c = new(singleClient)
			x.clients[strAddr] = c
		}
		x.mtx.Unlock()
	}

	c.Lock()
	defer c.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	if x.opts.ReconnectTimeout != 0 && time.Since(c.lastAttempt) < x.opts.ReconnectTimeout {
		return nil, errRecentlyFailed
	}

	cl, err := x.createForAddress(addr)
	if err != nil {
		c.lastAttempt = time.Now()
		return nil, err
	}

	c.client = cl
	return cl, nil
}
