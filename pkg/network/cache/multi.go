package cache

import (
	"context"
	"errors"
	"sync"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

type multiClient struct {
	mtx sync.RWMutex

	clients map[string]clientcore.Client

	addr network.AddressGroup

	opts ClientCacheOpts
}

func newMultiClient(addr network.AddressGroup, opts ClientCacheOpts) *multiClient {
	return &multiClient{
		clients: make(map[string]clientcore.Client),
		addr:    addr,
		opts:    opts,
	}
}

// note: must be wrapped into mutex lock.
func (x *multiClient) createForAddress(addr network.Address) clientcore.Client {
	var (
		c       client.Client
		prmInit client.PrmInit
		prmDial client.PrmDial
	)

	prmDial.SetServerURI(addr.URIAddr())

	if x.opts.Key != nil {
		prmInit.SetDefaultPrivateKey(*x.opts.Key)
	}

	if x.opts.DialTimeout > 0 {
		prmDial.SetTimeout(x.opts.DialTimeout)
	}

	if x.opts.ResponseCallback != nil {
		prmInit.SetResponseInfoCallback(x.opts.ResponseCallback)
	}

	c.Init(prmInit)
	err := c.Dial(prmDial)
	if err != nil {
		// client never returns an error
		panic(err)
	}

	x.clients[addr.String()] = &c

	return &c
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
	x.addr = group
}

func (x *multiClient) iterateClients(ctx context.Context, f func(clientcore.Client) error) error {
	var firstErr error

	x.addr.IterateAddresses(func(addr network.Address) bool {
		select {
		case <-ctx.Done():
			firstErr = context.Canceled
			return true
		default:
		}

		var err error

		c := x.client(addr)

		err = f(c)

		success := err == nil || errors.Is(err, context.Canceled)

		if success || firstErr == nil {
			firstErr = err
		}

		return success
	})

	return firstErr
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
			_ = c.Close()
		}
	}

	x.mtx.RUnlock()

	return nil
}

func (x *multiClient) RawForAddress(addr network.Address, f func(client *rawclient.Client) error) error {
	return x.client(addr).ExecRaw(f)
}

func (x *multiClient) client(addr network.Address) clientcore.Client {
	strAddr := addr.String()

	x.mtx.RLock()
	c, cached := x.clients[strAddr]
	x.mtx.RUnlock()

	if !cached {
		x.mtx.Lock()

		c, cached = x.clients[strAddr]
		if !cached {
			c = x.createForAddress(addr)
		}
		x.mtx.Unlock()
	}

	return c
}
