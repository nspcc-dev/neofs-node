package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/uriutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Clients manages connections to remote SNs.
type Clients struct {
	log *zap.Logger

	// NeoFS client settings
	streamMsgTimeout time.Duration
	signBufPool      *sync.Pool
	// gRPC settings
	minConnTimeout time.Duration
	pingInterval   time.Duration
	pingTimeout    time.Duration

	mtx   sync.RWMutex
	conns map[string]*connections // keys are public key bytes
}

// NewClients constructs Clients initializing connection to any endpoint with
// given parameters.
func NewClients(l *zap.Logger, signBufPool *sync.Pool, streamTimeout, minConnTimeout, pingInterval, pingTimeout time.Duration) *Clients {
	return &Clients{
		log:              l,
		streamMsgTimeout: streamTimeout,
		signBufPool:      signBufPool,
		minConnTimeout:   minConnTimeout,
		pingInterval:     pingInterval,
		pingTimeout:      pingTimeout,
		conns:            make(map[string]*connections),
	}
}

// CloseAll closes all opened connections.
func (x *Clients) CloseAll() {
	x.mtx.RLock()
	defer x.mtx.RUnlock()
	for _, c := range x.conns {
		c.closeAll()
	}
}

func snCacheKey(pub []byte) string { return string(pub) }

// Get initializes connections to network addresses of described SN and returns
// interface to access them. All opened connections are cached and kept alive
// until [Clients.CloseAll].
func (x *Clients) Get(info clientcore.NodeInfo) (clientcore.MultiAddressClient, error) {
	pub := info.PublicKey()
	cacheKey := snCacheKey(pub)

	x.mtx.RLock()
	c, ok := x.conns[cacheKey]
	x.mtx.RUnlock()
	if ok {
		return c, nil
	}

	x.mtx.Lock()
	defer x.mtx.Unlock()
	if c, ok = x.conns[cacheKey]; ok {
		return c, nil
	}

	c, err := x.initConnections(pub, info.AddressGroup())
	if err != nil {
		return nil, fmt.Errorf("init connections: %w", err)
	}
	x.conns[cacheKey] = c
	return c, nil
}

// SyncWithNewNetmap synchronizes x with the passed new network map.
func (x *Clients) SyncWithNewNetmap(sns []netmap.NodeInfo, local int) {
	x.mtx.Lock()
	defer x.mtx.Unlock()

	for i := range sns {
		if i == local {
			continue
		}
		if err := x.syncWithNetmapSN(sns[i]); err != nil {
			x.log.Warn("failed to sync connection cache with SN from the new network map, skip",
				zap.String("pub", hex.EncodeToString(sns[i].PublicKey())), zap.Error(err))
		}
	}

	maps.DeleteFunc(x.conns, func(pub string, conns *connections) bool {
		if slices.ContainsFunc(sns, func(sn netmap.NodeInfo) bool { return string(sn.PublicKey()) == pub }) {
			return false
		}
		l := x.log.With(zap.String("public key", hex.EncodeToString([]byte(pub))))
		l.Info("closing all connections of SN not present in the new network map...")
		conns.closeAll()
		l.Info("finished closing all connections of SN not present in the new network map")
		return true
	})
}

func (x *Clients) syncWithNetmapSN(sn netmap.NodeInfo) error {
	pub := sn.PublicKey()
	conns, ok := x.conns[snCacheKey(pub)]
	if !ok {
		return nil
	}

	as := make(network.AddressGroup, 0, sn.NumberOfNetworkEndpoints())
	var a network.Address
	for ma := range sn.NetworkEndpoints() {
		if err := a.FromString(ma); err != nil {
			// TODO: if at least one address is OK, SN can be operational
			return fmt.Errorf("parse network address %q: %w", ma, err)
		}
		as = append(as, a)
	}

	conns.mtx.Lock()
	defer conns.mtx.Unlock()

	maps.DeleteFunc(conns.m, func(ma string, c *client.Client) bool {
		if slices.ContainsFunc(as, func(a network.Address) bool { return a.String() == ma }) {
			return false
		}
		if err := c.Close(); err != nil {
			x.log.Info("failed to close connection to the SN address no longer present in the new network map",
				zap.String("address", ma), zap.Error(err))
			// nothing else can be done, leave for GC
			return true
		}
		x.log.Info("connection to the SN address no longer present in the new network map successfully closed", zap.String("address", ma))
		return true
	})

	for i := range as {
		ma := as[i].String()
		if _, ok := conns.m[ma]; ok {
			continue
		}
		x.log.Info("initializing connection to new SN address in the new network map...", zap.String("address", ma))
		c, err := x.initConnection(pub, as[i].URIAddr())
		if err != nil {
			x.log.Info("failed to init connection to new SN address in the new network map",
				zap.String("address", ma), zap.Error(err))
			continue
		}
		conns.m[ma] = c
		x.log.Info("connection to new SN address in the new network map successfully initialized", zap.String("address", ma))
	}

	return nil
}

func (x *Clients) initConnections(pub []byte, as network.AddressGroup) (*connections, error) {
	m := make(map[string]*client.Client, len(as))
	l := x.log.With(zap.String("public key", hex.EncodeToString(pub)))
	for i := range as {
		cacheKey := as[i].String()
		l.Info("initializing connection to the SN...", zap.String("address", cacheKey))
		c, err := x.initConnection(pub, as[i].URIAddr())
		if err != nil {
			// TODO: if at least one address is OK, SN can be operational
			for cl := range maps.Values(m) {
				_ = cl.Close()
			}
			return nil, fmt.Errorf("init conn to %q: %w", as[i], err)
		}
		l.Info("connection to the SN successfully initialized", zap.String("address", cacheKey))
		m[cacheKey] = c
	}
	return &connections{
		log: x.log.With(zap.String("SN public key", hex.EncodeToString(pub))),
		m:   m,
	}, nil
}

func (x *Clients) initConnection(pub []byte, uri string) (*client.Client, error) {
	target, withTLS, err := uriutil.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("parse URI: %w", err)
	}
	var transportCreds credentials.TransportCredentials
	if withTLS {
		transportCreds = credentials.NewTLS(nil)
	} else {
		transportCreds = insecure.NewCredentials()
	}
	grpcConn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: x.minConnTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                x.pingInterval,
			Timeout:             x.pingTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil { // should never happen
		return nil, fmt.Errorf("init gRPC client conn: %w", err)
	}
	res, err := client.NewGRPC(grpcConn, x.signBufPool, x.streamMsgTimeout, func(respPub []byte) error {
		if !bytes.Equal(respPub, pub) {
			return clientcore.ErrWrongPublicKey
		}
		return nil
	})
	if err != nil {
		_ = grpcConn.Close()
		return res, fmt.Errorf("init NeoFS API client from gRPC client conn: %w", err)
	}
	grpcConn.Connect()
	return res, nil
}

type connections struct {
	log *zap.Logger

	mtx sync.RWMutex
	m   map[string]*client.Client // keys are multiaddrs
}

func (x *connections) closeAll() {
	for ma, c := range x.all {
		if err := c.Close(); err != nil {
			x.log.Info("failed to close connection", zap.String("address", ma), zap.Error(err))
			continue
		}
		x.log.Info("connection successfully closed", zap.String("address", ma))
	}
}

func (x *connections) all(f func(ma string, c *client.Client) bool) {
	x.mtx.RLock()
	maps.All(x.m)(f)
	x.mtx.RUnlock()
}

func (x *connections) forEach(ctx context.Context, f func(context.Context, *client.Client) error) error {
	var firstErr error
	for ma, c := range x.all {
		err := f(ctx, c)
		if err == nil {
			return nil
		}
		if !isTempError(err) {
			return newEndpointError(ma, err)
		}
		if firstErr == nil {
			firstErr = newEndpointError(ma, err)
		}
	}
	return newMultiEndpointError(firstErr)
}

func (x *connections) ForEachGRPCConn(ctx context.Context, f func(context.Context, *grpc.ClientConn) error) error {
	return x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		return f(ctx, c.Conn())
	})
}

func (x *connections) ObjectPutInit(ctx context.Context, hdr object.Object, signer user.Signer, opts client.PrmObjectPutInit) (client.ObjectWriter, error) {
	var res client.ObjectWriter
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectPutInit(ctx, hdr, signer, opts)
		return err
	})
}

func (x *connections) ReplicateObject(ctx context.Context, id oid.ID, src io.ReadSeeker, signer neofscrypto.Signer, signedReplication bool) (*neofscrypto.Signature, error) {
	// same as forEach but with specific error handling
	var firstErr error
	for ma, c := range x.all {
		sig, err := c.ReplicateObject(ctx, id, src, signer, signedReplication)
		if err == nil {
			return sig, nil
		}
		if !isTempError(err) {
			return nil, newEndpointError(ma, err)
		}
		if _, errSeek := src.Seek(0, io.SeekStart); errSeek != nil {
			return nil, fmt.Errorf("seek start: %w", errSeek)
		}
		if firstErr == nil {
			firstErr = newEndpointError(ma, err)
		}
	}
	return nil, newMultiEndpointError(firstErr)
}

func (x *connections) ObjectDelete(ctx context.Context, cnr cid.ID, obj oid.ID, signer user.Signer, opts client.PrmObjectDelete) (oid.ID, error) {
	var res oid.ID
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectDelete(ctx, cnr, obj, signer, opts)
		return err
	})
}

func (x *connections) ObjectGetInit(ctx context.Context, cnr cid.ID, id oid.ID, signer user.Signer, opts client.PrmObjectGet) (object.Object, *client.PayloadReader, error) {
	var res1 object.Object
	var res2 *client.PayloadReader
	return res1, res2, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res1, res2, err = c.ObjectGetInit(ctx, cnr, id, signer, opts)
		return err
	})
}

func (x *connections) ObjectHead(ctx context.Context, cnr cid.ID, id oid.ID, signer user.Signer, opts client.PrmObjectHead) (*object.Object, error) {
	var res *object.Object
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectHead(ctx, cnr, id, signer, opts)
		return err
	})
}

func (x *connections) ObjectSearchInit(ctx context.Context, cnr cid.ID, signer user.Signer, opts client.PrmObjectSearch) (*client.ObjectListReader, error) {
	var res *client.ObjectListReader
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectSearchInit(ctx, cnr, signer, opts)
		return err
	})
}

func (x *connections) ObjectRangeInit(ctx context.Context, cnr cid.ID, id oid.ID, off, ln uint64, signer user.Signer, opts client.PrmObjectRange) (*client.ObjectRangeReader, error) {
	var res *client.ObjectRangeReader
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectRangeInit(ctx, cnr, id, off, ln, signer, opts)
		return err
	})
}

func (x *connections) ObjectHash(ctx context.Context, cnr cid.ID, id oid.ID, signer user.Signer, opts client.PrmObjectHash) ([][]byte, error) {
	var res [][]byte
	return res, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		res, err = c.ObjectHash(ctx, cnr, id, signer, opts)
		return err
	})
}

func (x *connections) AnnounceLocalTrust(ctx context.Context, epoch uint64, ts []apireputation.Trust, opts client.PrmAnnounceLocalTrust) error {
	return x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		return c.AnnounceLocalTrust(ctx, epoch, ts, opts)
	})
}

func (x *connections) AnnounceIntermediateTrust(ctx context.Context, epoch uint64, t apireputation.PeerToPeerTrust, opts client.PrmAnnounceIntermediateTrust) error {
	return x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		return c.AnnounceIntermediateTrust(ctx, epoch, t, opts)
	})
}

func (x *connections) SearchObjects(ctx context.Context, cnr cid.ID, fs object.SearchFilters, attrs []string, cursor string,
	signer neofscrypto.Signer, opts client.SearchObjectsOptions) ([]client.SearchResultItem, string, error) {
	var resItems []client.SearchResultItem
	var resCursor string
	return resItems, resCursor, x.forEach(ctx, func(ctx context.Context, c *client.Client) error {
		var err error
		resItems, resCursor, err = c.SearchObjects(ctx, cnr, fs, attrs, cursor, signer, opts)
		return err
	})
}

func isTempError(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

func newEndpointError(addr string, err error) error {
	return fmt.Errorf("%s: %w", addr, err)
}

func newMultiEndpointError(first error) error {
	return fmt.Errorf("all endpoints failed, first error: %w", first)
}
