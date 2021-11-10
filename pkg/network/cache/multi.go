package cache

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"io"
	"sync"

	rawclient "github.com/nspcc-dev/neofs-api-go/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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

func (x *multiClient) PutObject(ctx context.Context, p *client.PutObjectParams, opts ...client.CallOption) (*object.ID, error) {
	var res *object.ID

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.PutObject(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) GetBalance(ctx context.Context, id *owner.ID, opts ...client.CallOption) (*accounting.Decimal, error) {
	var res *accounting.Decimal

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.GetBalance(ctx, id, opts...)
		return
	})

	return res, err
}

func (x *multiClient) PutContainer(ctx context.Context, cnr *container.Container, opts ...client.CallOption) (*cid.ID, error) {
	var res *cid.ID

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.PutContainer(ctx, cnr, opts...)
		return
	})

	return res, err
}

func (x *multiClient) GetContainer(ctx context.Context, id *cid.ID, opts ...client.CallOption) (*container.Container, error) {
	var res *container.Container

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.GetContainer(ctx, id, opts...)
		return
	})

	return res, err
}

func (x *multiClient) ListContainers(ctx context.Context, id *owner.ID, opts ...client.CallOption) ([]*cid.ID, error) {
	var res []*cid.ID

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.ListContainers(ctx, id, opts...)
		return
	})

	return res, err
}

func (x *multiClient) DeleteContainer(ctx context.Context, id *cid.ID, opts ...client.CallOption) error {
	return x.iterateClients(ctx, func(c client.Client) error {
		return c.DeleteContainer(ctx, id, opts...)
	})
}

func (x *multiClient) GetEACL(ctx context.Context, id *cid.ID, opts ...client.CallOption) (*client.EACLWithSignature, error) {
	var res *client.EACLWithSignature

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.GetEACL(ctx, id, opts...)
		return
	})

	return res, err
}

func (x *multiClient) SetEACL(ctx context.Context, t *eacl.Table, opts ...client.CallOption) error {
	return x.iterateClients(ctx, func(c client.Client) error {
		return c.SetEACL(ctx, t, opts...)
	})
}

func (x *multiClient) AnnounceContainerUsedSpace(ctx context.Context, as []container.UsedSpaceAnnouncement, opts ...client.CallOption) error {
	return x.iterateClients(ctx, func(c client.Client) error {
		return c.AnnounceContainerUsedSpace(ctx, as, opts...)
	})
}

func (x *multiClient) EndpointInfo(ctx context.Context, opts ...client.CallOption) (*client.EndpointInfo, error) {
	var res *client.EndpointInfo

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.EndpointInfo(ctx, opts...)
		return
	})

	return res, err
}

func (x *multiClient) NetworkInfo(ctx context.Context, opts ...client.CallOption) (*netmap.NetworkInfo, error) {
	var res *netmap.NetworkInfo

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.NetworkInfo(ctx, opts...)
		return
	})

	return res, err
}

func (x *multiClient) DeleteObject(ctx context.Context, p *client.DeleteObjectParams, opts ...client.CallOption) error {
	return x.iterateClients(ctx, func(c client.Client) error {
		return c.DeleteObject(ctx, p, opts...)
	})
}

func (x *multiClient) GetObject(ctx context.Context, p *client.GetObjectParams, opts ...client.CallOption) (*object.Object, error) {
	var res *object.Object

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.GetObject(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) GetObjectHeader(ctx context.Context, p *client.ObjectHeaderParams, opts ...client.CallOption) (*object.Object, error) {
	var res *object.Object

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.GetObjectHeader(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) ObjectPayloadRangeData(ctx context.Context, p *client.RangeDataParams, opts ...client.CallOption) ([]byte, error) {
	var res []byte

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.ObjectPayloadRangeData(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) ObjectPayloadRangeSHA256(ctx context.Context, p *client.RangeChecksumParams, opts ...client.CallOption) ([][sha256.Size]byte, error) {
	var res [][sha256.Size]byte

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.ObjectPayloadRangeSHA256(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) ObjectPayloadRangeTZ(ctx context.Context, p *client.RangeChecksumParams, opts ...client.CallOption) ([][client.TZSize]byte, error) {
	var res [][client.TZSize]byte

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.ObjectPayloadRangeTZ(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) SearchObject(ctx context.Context, p *client.SearchObjectParams, opts ...client.CallOption) ([]*object.ID, error) {
	var res []*object.ID

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.SearchObject(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) CreateSession(ctx context.Context, exp uint64, opts ...client.CallOption) (*session.Token, error) {
	var res *session.Token

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.CreateSession(ctx, exp, opts...)
		return
	})

	return res, err
}

func (x *multiClient) AnnounceLocalTrust(ctx context.Context, p client.AnnounceLocalTrustPrm, opts ...client.CallOption) (*client.AnnounceLocalTrustRes, error) {
	var res *client.AnnounceLocalTrustRes

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.AnnounceLocalTrust(ctx, p, opts...)
		return
	})

	return res, err
}

func (x *multiClient) AnnounceIntermediateTrust(ctx context.Context, p client.AnnounceIntermediateTrustPrm, opts ...client.CallOption) (*client.AnnounceIntermediateTrustRes, error) {
	var res *client.AnnounceIntermediateTrustRes

	err := x.iterateClients(ctx, func(c client.Client) (err error) {
		res, err = c.AnnounceIntermediateTrust(ctx, p, opts...)
		return
	})

	return res, err
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
