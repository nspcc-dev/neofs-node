package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

type multiClient struct {
	mtx sync.RWMutex

	clients map[string]client.Client

	addr network.AddressGroup

	opts []client.Option
}

func newMultiClient(addr network.AddressGroup, opts []client.Option) *multiClient {
	return &multiClient{
		clients: make(map[string]client.Client),
		addr:    addr,
		opts:    opts,
	}
}

// note: must be wrapped into mutex lock.
func (x *multiClient) createForAddress(addr network.Address) client.Client {
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

func (x *multiClient) iterateClients(ctx context.Context, f func(client.Client) error) error {
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
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.PutObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) GetBalance(ctx context.Context, id *owner.ID, opts ...client.CallOption) (res *client.BalanceOfRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.GetBalance(ctx, id, opts...)
		return err
	})

	return
}

func (x *multiClient) PutContainer(ctx context.Context, cnr *container.Container, opts ...client.CallOption) (res *client.ContainerPutRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.PutContainer(ctx, cnr, opts...)
		return err
	})

	return
}

func (x *multiClient) GetContainer(ctx context.Context, id *cid.ID, opts ...client.CallOption) (res *client.ContainerGetRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.GetContainer(ctx, id, opts...)
		return err
	})

	return
}

func (x *multiClient) ListContainers(ctx context.Context, id *owner.ID, opts ...client.CallOption) (res *client.ContainerListRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.ListContainers(ctx, id, opts...)
		return err
	})

	return
}

func (x *multiClient) DeleteContainer(ctx context.Context, id *cid.ID, opts ...client.CallOption) (res *client.ContainerDeleteRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.DeleteContainer(ctx, id, opts...)
		return err
	})

	return
}

func (x *multiClient) EACL(ctx context.Context, id *cid.ID, opts ...client.CallOption) (res *client.EACLRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.EACL(ctx, id, opts...)
		return err
	})

	return
}

func (x *multiClient) SetEACL(ctx context.Context, t *eacl.Table, opts ...client.CallOption) (res *client.SetEACLRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.SetEACL(ctx, t, opts...)
		return err
	})

	return
}

func (x *multiClient) AnnounceContainerUsedSpace(ctx context.Context, as []container.UsedSpaceAnnouncement, opts ...client.CallOption) (res *client.AnnounceSpaceRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.AnnounceContainerUsedSpace(ctx, as, opts...)
		return err
	})

	return
}

func (x *multiClient) EndpointInfo(ctx context.Context, opts ...client.CallOption) (res *client.EndpointInfoRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.EndpointInfo(ctx, opts...)
		return err
	})

	return
}

func (x *multiClient) NetworkInfo(ctx context.Context, opts ...client.CallOption) (res *client.NetworkInfoRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.NetworkInfo(ctx, opts...)
		return err
	})

	return
}

func (x *multiClient) DeleteObject(ctx context.Context, p *client.DeleteObjectParams, opts ...client.CallOption) (res *client.ObjectDeleteRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.DeleteObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) GetObject(ctx context.Context, p *client.GetObjectParams, opts ...client.CallOption) (res *client.ObjectGetRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.GetObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) ObjectPayloadRangeData(ctx context.Context, p *client.RangeDataParams, opts ...client.CallOption) (res *client.ObjectRangeRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.ObjectPayloadRangeData(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) HeadObject(ctx context.Context, p *client.ObjectHeaderParams, opts ...client.CallOption) (res *client.ObjectHeadRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.HeadObject(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) HashObjectPayloadRanges(ctx context.Context, p *client.RangeChecksumParams, opts ...client.CallOption) (res *client.ObjectRangeHashRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.HashObjectPayloadRanges(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) SearchObjects(ctx context.Context, p *client.SearchObjectParams, opts ...client.CallOption) (res *client.ObjectSearchRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.SearchObjects(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) CreateSession(ctx context.Context, exp uint64, opts ...client.CallOption) (res *client.CreateSessionRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.CreateSession(ctx, exp, opts...)
		return err
	})

	return
}

func (x *multiClient) AnnounceLocalTrust(ctx context.Context, p client.AnnounceLocalTrustPrm, opts ...client.CallOption) (res *client.AnnounceLocalTrustRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.AnnounceLocalTrust(ctx, p, opts...)
		return err
	})

	return
}

func (x *multiClient) AnnounceIntermediateTrust(ctx context.Context, p client.AnnounceIntermediateTrustPrm, opts ...client.CallOption) (res *client.AnnounceIntermediateTrustRes, err error) {
	err = x.iterateClients(ctx, func(c client.Client) error {
		res, err = c.AnnounceIntermediateTrust(ctx, p, opts...)
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

func (x *multiClient) client(addr network.Address) client.Client {
	x.mtx.Lock()

	strAddr := addr.String()

	c, cached := x.clients[strAddr]
	if !cached {
		c = x.createForAddress(addr)
	}

	x.mtx.Unlock()

	return c
}
