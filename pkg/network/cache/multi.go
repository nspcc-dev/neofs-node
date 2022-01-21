package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
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

	opts []client.Option
}

func newMultiClient(addr network.AddressGroup, opts []client.Option) *multiClient {
	return &multiClient{
		clients: make(map[string]clientcore.Client),
		addr:    addr,
		opts:    opts,
	}
}

// note: must be wrapped into mutex lock.
func (x *multiClient) createForAddress(addr network.Address) clientcore.Client {
	opts := append(x.opts, client.WithAddress(addr.HostAddr()))

	if addr.TLSEnabled() {
		opts = append(opts, client.WithTLSConfig(&tls.Config{}))
	}

	c, err := client.New(opts...)
	if err != nil {
		// client never returns an error
		panic(err)
	}

	x.clients[addr.String()] = c

	return c
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

func (x *multiClient) PutObject(ctx context.Context, p *client.PutObjectParams, opts ...client.CallOption) (res *client.ObjectPutRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.PutObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) AnnounceContainerUsedSpace(ctx context.Context, prm client.AnnounceSpacePrm) (res *client.AnnounceSpaceRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.AnnounceContainerUsedSpace(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) DeleteObject(ctx context.Context, p *client.DeleteObjectParams, opts ...client.CallOption) (res *client.ObjectDeleteRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.DeleteObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) GetObject(ctx context.Context, p *client.GetObjectParams, opts ...client.CallOption) (res *client.ObjectGetRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.GetObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) ObjectPayloadRangeData(ctx context.Context, p *client.RangeDataParams, opts ...client.CallOption) (res *client.ObjectRangeRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectPayloadRangeData(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) HeadObject(ctx context.Context, p *client.ObjectHeaderParams, opts ...client.CallOption) (res *client.ObjectHeadRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.HeadObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) HashObjectPayloadRanges(ctx context.Context, p *client.RangeChecksumParams, opts ...client.CallOption) (res *client.ObjectRangeHashRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.HashObjectPayloadRanges(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) SearchObjects(ctx context.Context, p *client.SearchObjectParams, opts ...client.CallOption) (res *client.ObjectSearchRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.SearchObjects(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) AnnounceLocalTrust(ctx context.Context, prm client.AnnounceLocalTrustPrm) (res *client.AnnounceLocalTrustRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.AnnounceLocalTrust(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) AnnounceIntermediateTrust(ctx context.Context, prm client.AnnounceIntermediateTrustPrm) (res *client.AnnounceIntermediateTrustRes, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.AnnounceIntermediateTrust(ctx, prm)
		return err
	})

	return
}

func (x *multiClient) Raw() *rawclient.Client {
	panic("multiClient.Raw() must not be called")
}

func (x *multiClient) Conn() io.Closer {
	return x
}

func (x *multiClient) Close() error {
	x.mtx.RLock()

	{
		for _, c := range x.clients {
			_ = c.Conn().Close()
		}
	}

	x.mtx.RUnlock()

	return nil
}

func (x *multiClient) RawForAddress(addr network.Address) *rawclient.Client {
	return x.client(addr).Raw()
}

func (x *multiClient) client(addr network.Address) clientcore.Client {
	x.mtx.Lock()

	strAddr := addr.String()

	c, cached := x.clients[strAddr]
	if !cached {
		c = x.createForAddress(addr)
	}

	x.mtx.Unlock()

	return c
}
