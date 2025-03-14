package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	reputationSDK "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

type clientWrapper struct {
	*client.Client
}

func (x *multiClient) createForAddress(addr network.Address) (clientcore.Client, error) {
	var (
		prmInit client.PrmInit
		prmDial client.PrmDial
	)

	prmInit.SetSignMessageBuffers(x.opts.Buffers)

	endpoint := addr.URIAddr()
	prmDial.SetServerURI(endpoint)

	if x.opts.DialTimeout > 0 {
		prmDial.SetTimeout(x.opts.DialTimeout)
	}

	if x.opts.StreamTimeout > 0 {
		prmDial.SetStreamTimeout(x.opts.StreamTimeout)
	}

	if x.opts.ResponseCallback != nil {
		prmInit.SetResponseInfoCallback(x.opts.ResponseCallback)
	}

	c, err := client.New(prmInit)
	if err != nil {
		return nil, fmt.Errorf("can't create SDK client: %w", err)
	}

	st := time.Now()
	x.opts.Logger.Info("dialing node endpoint...", zap.String("endpoint", endpoint), zap.Stringer("dial timeout", x.opts.DialTimeout),
		zap.Stringer("stream timeout", x.opts.StreamTimeout))
	err = c.Dial(prmDial)
	if err != nil {
		x.opts.Logger.Info("node endpoint dial failed", zap.String("endpoint", endpoint), zap.Stringer("took", time.Since(st)), zap.Error(err))
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}
	x.opts.Logger.Info("node endpoint dial succeeded", zap.String("endpoint", endpoint), zap.Stringer("took", time.Since(st)))

	return clientWrapper{c}, nil
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

	x.opts.Logger.Info("node address group needs an update", zap.String("old", network.StringifyGroup(oldGroup)), zap.String("new", network.StringifyGroup(group)))

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
		x.opts.Logger.Info("dropped conn to the node", zap.String("address", a))
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
			x.ReportError(addr, err)
		}

		return success
	})

	return firstErr
}

func (x *multiClient) ReportError(addr network.Address, err error) {
	if errors.Is(err, errRecentlyFailed) {
		x.opts.Logger.Info("node endpoint recently failed, skip invalidation",
			zap.Stringer("cause address", addr), zap.Error(err))
		return
	}

	if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
		x.opts.Logger.Info("node endpoint failed with cancelled context, skip invalidation",
			zap.Stringer("cause address", addr), zap.Error(err))
		return
	}

	// NeoFS status responses mean connection is OK
	if errors.Is(err, apistatus.Error) {
		x.opts.Logger.Info("node endpoint failed with API status, skip invalidation",
			zap.Stringer("cause address", addr), zap.Error(err))
		return
	}

	// non-status logic error that could be returned
	// from the SDK client; should not be considered
	// as a connection error
	var siErr *objectSDK.SplitInfoError
	if errors.As(err, &siErr) {
		x.opts.Logger.Info("node endpoint failed with split info status, skip invalidation",
			zap.Stringer("cause address", addr))
		return
	}

	// Dropping all clients here is not necessary, we do this
	// because `multiClient` doesn't yet provide convenient interface
	// for reporting individual errors for streaming operations.
	x.mtx.RLock()

	x.addrMtx.RLock()
	group := x.addr
	x.addrMtx.RUnlock()

	for a, sc := range x.clients {
		sc.invalidate(x.opts.Logger.With(zap.String("address", a), zap.Stringer("cause address", addr), zap.String("cause", err.Error())))
	}

	x.mtx.RUnlock()

	x.opts.Logger.Info("invalidated cached clients to the node caused by the error",
		zap.Stringers("address group", group), zap.Stringer("cause address", addr), zap.Error(err))
}

func (s *singleClient) invalidate(l *zap.Logger) {
	s.Lock()
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			l.Info("node endpoint conn close failed", zap.Error(err))
		} else {
			l.Info("node endpoint conn close succeeded")
		}
	}
	s.client = nil
	s.Unlock()
}

func (x *multiClient) ObjectPutInit(ctx context.Context, header objectSDK.Object, signer user.Signer, p client.PrmObjectPutInit) (res client.ObjectWriter, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectPutInit(ctx, header, signer, p)
		return err
	})

	return
}

func (x *multiClient) ReplicateObject(ctx context.Context, id oid.ID, src io.ReadSeeker, signer neofscrypto.Signer, signedReplication bool) (*neofscrypto.Signature, error) {
	var errSeek error
	var signature *neofscrypto.Signature
	err := x.iterateClients(ctx, func(c clientcore.Client) error {
		var err error
		signature, err = c.ReplicateObject(ctx, id, src, signer, signedReplication)
		if err != nil {
			_, errSeek = src.Seek(0, io.SeekStart)
			if errSeek != nil {
				return nil // to break the iterator
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return signature, errSeek
}

func (x *multiClient) ContainerAnnounceUsedSpace(ctx context.Context, announcements []container.SizeEstimation, prm client.PrmAnnounceSpace) error {
	return x.iterateClients(ctx, func(c clientcore.Client) error {
		return c.ContainerAnnounceUsedSpace(ctx, announcements, prm)
	})
}

func (x *multiClient) ObjectDelete(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectDelete) (tombID oid.ID, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		tombID, err = c.ObjectDelete(ctx, containerID, objectID, signer, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectGetInit(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectGet) (hdr objectSDK.Object, rdr *client.PayloadReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		hdr, rdr, err = c.ObjectGetInit(ctx, containerID, objectID, signer, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectRangeInit(ctx context.Context, containerID cid.ID, objectID oid.ID, offset, length uint64, signer user.Signer, prm client.PrmObjectRange) (res *client.ObjectRangeReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectRangeInit(ctx, containerID, objectID, offset, length, signer, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHead) (res *objectSDK.Object, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectHead(ctx, containerID, objectID, signer, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectHash(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHash) (res [][]byte, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectHash(ctx, containerID, objectID, signer, prm)
		return err
	})

	return
}

func (x *multiClient) ObjectSearchInit(ctx context.Context, containerID cid.ID, signer user.Signer, prm client.PrmObjectSearch) (res *client.ObjectListReader, err error) {
	err = x.iterateClients(ctx, func(c clientcore.Client) error {
		res, err = c.ObjectSearchInit(ctx, containerID, signer, prm)
		return err
	})

	return
}

func (x *multiClient) AnnounceLocalTrust(ctx context.Context, epoch uint64, trusts []reputationSDK.Trust, prm client.PrmAnnounceLocalTrust) error {
	return x.iterateClients(ctx, func(c clientcore.Client) error {
		return c.AnnounceLocalTrust(ctx, epoch, trusts, prm)
	})
}

func (x *multiClient) AnnounceIntermediateTrust(ctx context.Context, epoch uint64, trust reputationSDK.PeerToPeerTrust, prm client.PrmAnnounceIntermediateTrust) error {
	return x.iterateClients(ctx, func(c clientcore.Client) error {
		return c.AnnounceIntermediateTrust(ctx, epoch, trust, prm)
	})
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

func (x *multiClient) RawForAddress(addr network.Address, f func(*grpc.ClientConn) error) error {
	c, err := x.client(addr)
	if err != nil {
		return err
	}

	err = f(c.Conn())
	if err != nil {
		x.ReportError(addr, err)
	}
	return err
}

func (x *multiClient) Conn() *grpc.ClientConn {
	var cc *grpc.ClientConn

	_ = x.iterateClients(context.TODO(), func(c clientcore.Client) error {
		cc = c.Conn()
		return nil
	})
	return cc
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
		passed := time.Since(c.lastAttempt)
		if x.opts.ReconnectTimeout != 0 && passed < x.opts.ReconnectTimeout {
			c.RUnlock()
			x.opts.Logger.Info("node endpoint unavailability not timed out yet",
				zap.String("endpoint", strAddr), zap.Stringer("last try", c.lastAttempt), zap.Stringer("passed", passed))
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

	passed := time.Since(c.lastAttempt)
	if x.opts.ReconnectTimeout != 0 && passed < x.opts.ReconnectTimeout {
		x.opts.Logger.Info("node unavailability after previous failure not timed out yet",
			zap.String("endpoint", strAddr), zap.Stringer("last try", c.lastAttempt), zap.Stringer("passed", passed))
		return nil, errRecentlyFailed
	}

	x.opts.Logger.Info("node endpoint unavailability timed out, retrying...",
		zap.String("endpoint", strAddr), zap.Stringer("last try", c.lastAttempt))

	cl, err := x.createForAddress(addr)
	if err != nil {
		c.lastAttempt = time.Now()
		x.opts.Logger.Info("marked node endpoint unavailable for some time, will retry after timeout",
			zap.String("endpoint", strAddr), zap.Stringer("now", c.lastAttempt), zap.Stringer("timeout", x.opts.ReconnectTimeout))
		return nil, err
	}

	x.opts.Logger.Info("successfully connected to the node endpoint",
		zap.String("endpoint", strAddr), zap.Stringer("previous attempt", c.lastAttempt))

	c.client = cl
	return cl, nil
}
