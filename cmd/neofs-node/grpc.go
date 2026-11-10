package main

import (
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net"
	"os"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
)

const maxTLSFingerprintFileBytes int64 = 16 << 10 // 16 KB

type grpcServerSnapshot struct {
	grpcconfig.GRPC
	certFingerprint string
}

func (s grpcServerSnapshot) unchanged(other grpcServerSnapshot) bool {
	return s.ConnLimit == other.ConnLimit &&
		s.TLS.Enabled == other.TLS.Enabled &&
		s.TLS.Certificate == other.TLS.Certificate &&
		s.certFingerprint == other.certFingerprint
}

type grpcConfigSnapshot []grpcServerSnapshot

func selfSignedTLSConfigChanged(oldCfg, newCfg grpcConfigSnapshot) bool {
	oldByEndpoint := make(map[string]grpcServerSnapshot, len(oldCfg))
	for _, old := range oldCfg {
		oldByEndpoint[old.Endpoint] = old
	}
	for _, newSnap := range newCfg {
		old, ok := oldByEndpoint[newSnap.Endpoint]
		if !ok {
			if isSelfSignedTLS(newSnap.TLS) {
				return true
			}
			continue
		}
		delete(oldByEndpoint, newSnap.Endpoint)
		if isSelfSignedTLS(old.TLS) != isSelfSignedTLS(newSnap.TLS) {
			return true
		}
	}
	for _, old := range oldByEndpoint {
		if isSelfSignedTLS(old.TLS) {
			return true
		}
	}
	return false
}

func writeGRPCConfig(c *config.Config) grpcConfigSnapshot {
	snap := make(grpcConfigSnapshot, len(c.GRPC))
	for i, sc := range c.GRPC {
		snap[i] = grpcServerSnapshot{
			GRPC:            sc,
			certFingerprint: tlsCertFingerprint(sc.TLS.Certificate),
		}
	}
	return snap
}

func tlsCertFingerprint(certFile string) string {
	if certFile == "" {
		return ""
	}

	h := sha256.New()
	if err := hashFileLimited(h, certFile, maxTLSFingerprintFileBytes); err != nil {
		return ""
	}

	return string(h.Sum(nil))
}

func hashFileLimited(dst hash.Hash, path string, maxBytes int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = io.Copy(dst, io.LimitReader(f, maxBytes))
	if err != nil {
		return err
	}
	return nil
}

func initGRPC(c *cfg) {
	// although docs state that 'passthrough' is set by default, it should be set explicitly for activation
	resolver.SetDefaultScheme("passthrough")

	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	maxRecvMsgSizeOpt, err := getMaxRecvMsgSizeOpt(c)
	fatalOnErrDetails("read max object size network setting to determine gRPC recv message limit", err)

	if err := buildGRPCServers(c, maxRecvMsgSizeOpt); err != nil {
		fatalOnErr(err)
	}

	// register a single shutdown hook that stops whatever servers are current
	// at the time of shutdown (including those created by reload).
	c.onShutdown(func() {
		c.cfgGRPC.mu.Lock()
		srvs := make([]*grpc.Server, len(c.cfgGRPC.servers))
		for i, entry := range c.cfgGRPC.servers {
			srvs[i] = entry.srv
		}
		c.cfgGRPC.mu.Unlock()
		for _, srv := range srvs {
			stopGRPC("NeoFS Public API", srv, c.log)
		}
	})
}

// getMaxRecvMsgSizeOpt computes the grpc.MaxRecvMsgSize option based on the
// network's max object size setting. Returns nil when the default gRPC limit is
// sufficient.
func getMaxRecvMsgSizeOpt(c *cfg) (grpc.ServerOption, error) {
	maxObjSize, err := c.nCli.MaxObjectSize()
	if err != nil {
		return nil, err
	}

	// limit max size of single messages received by the gRPC servers up to max
	// object size setting of the NeoFS network: this is needed to serve
	// ObjectService.Replicate RPC transmitting the entire stored object in one
	// message
	maxRecvSize := maxObjSize
	// don't forget about meta fields
	if maxRecvSize < uint64(math.MaxUint64-object.MaxHeaderLen) { // just in case, always true in practice
		maxRecvSize += object.MaxHeaderLen
	} else {
		maxRecvSize = math.MaxUint64
	}

	if maxRecvSize <= maxMsgSize { // do not decrease default value
		return nil, nil
	}

	if maxRecvSize > math.MaxInt {
		// ^2GB for 32-bit systems which is currently enough in practice. If at some
		// point this is not enough, we'll need to expand the option
		return nil, fmt.Errorf("cannot serve NeoFS API over gRPC: object of max size is bigger than gRPC server is able to support %d>%d",
			maxRecvSize, math.MaxInt)
	}

	c.log.Debug("limit max recv gRPC message size to fit max stored objects",
		zap.Uint64("max object size", maxObjSize), zap.Uint64("max recv msg", maxRecvSize))

	return grpc.MaxRecvMsgSize(int(maxRecvSize)), nil
}

func buildGRPCServers(c *cfg, maxRecvMsgSizeOpt grpc.ServerOption) error {
	if len(c.appCfg.GRPC) == 0 {
		return errors.New("could not listen to any gRPC endpoints")
	}
	snaps := writeGRPCConfig(c.appCfg)
	for i, sc := range c.appCfg.GRPC {
		srv, lis, err := buildSingleGRPCServer(c, sc, maxRecvMsgSizeOpt)
		if err != nil {
			return err
		}
		c.cfgGRPC.servers = append(c.cfgGRPC.servers, cfgGRPCServer{snap: snaps[i], srv: srv, lis: lis})
	}
	return nil
}

func buildSingleGRPCServer(c *cfg, sc grpcconfig.GRPC, maxRecvMsgSizeOpt grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	serverOpts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // w/o this server sends GoAway with ENHANCE_YOUR_CALM code "too_many_pings"
			PermitWithoutStream: true,
		}),
		grpc.ReadBufferSize(256 * 1024),
		grpc.WriteBufferSize(256 * 1024),
		grpc.ForceServerCodecV2(iprotobuf.BufferedCodec{}),
	}
	if maxRecvMsgSizeOpt != nil {
		// TODO(@cthulhu-rider): the setting can be server-global only now, support
		//  per-RPC limits
		// TODO(@cthulhu-rider): max object size setting may change in general,
		//  but server configuration is static now
		serverOpts = append(serverOpts, maxRecvMsgSizeOpt)
	}

	tlsCfg := sc.TLS

	if isSelfSignedTLS(tlsCfg) {
		if c.selfSignedTLSCert == nil {
			return nil, nil, errors.New("missing self-signed TLS certificate")
		}
		serverOpts = append(serverOpts, grpc.Creds(trustedPeerTLSCredentials(&tls.Config{
			Certificates: []tls.Certificate{*c.selfSignedTLSCert},
			ClientAuth:   tls.RequestClientCert,
		})))
	} else if tlsCfg.Enabled {
		certFile := tlsCfg.Certificate

		if _, err := loadTLSCertificate(certFile, &c.key.PrivateKey); err != nil {
			c.log.Error("could not read certificate from file", zap.Error(err))
			return nil, nil, err
		}

		// read certificate from disk on each handshake to pick up renewals automatically.
		creds := trustedPeerTLSCredentials(&tls.Config{
			GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
				cert, err := loadTLSCertificate(certFile, &c.key.PrivateKey)
				if err != nil {
					return nil, fmt.Errorf("reload TLS certificate: %w", err)
				}
				return &tls.Config{
					Certificates: []tls.Certificate{cert},
					ClientAuth:   tls.RequestClientCert,
				}, nil
			},
		})

		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	lis, err := net.Listen("tcp", sc.Endpoint)
	if err != nil {
		c.log.Error("can't listen gRPC endpoint", zap.Error(err))
		return nil, nil, err
	}

	if connLimit := sc.ConnLimit; connLimit > 0 {
		lis = netutil.LimitListener(lis, connLimit)
	}

	return grpc.NewServer(serverOpts...), lis, nil
}

// reconcileGRPCServers decides, per endpoint in newCfg, whether to reuse the
// matching entry in old, stop it and build a replacement, or build one
// outright. stop is called for every old entry not reused; a changed
// endpoint is stopped before its replacement is built, so the address is
// released first.
//
// Matching is by endpoint name, never by position: old describes what is
// actually running, which a previous partial failure can leave smaller than
// newCfg.
//
// Returns the new server set and the subset of it that is freshly built and
// still needs service registration and serving.
func reconcileGRPCServers(old []cfgGRPCServer, newCfg grpcConfigSnapshot, build func(grpcServerSnapshot) (*grpc.Server, net.Listener, error), stop func(*grpc.Server)) (newServers []cfgGRPCServer, freshServers []*grpc.Server, freshListeners []net.Listener) {
	oldByEndpoint := make(map[string]cfgGRPCServer, len(old))
	for _, entry := range old {
		oldByEndpoint[entry.snap.Endpoint] = entry
	}

	newServers = make([]cfgGRPCServer, 0, len(newCfg))

	for _, newSnap := range newCfg {
		if oldEntry, ok := oldByEndpoint[newSnap.Endpoint]; ok {
			delete(oldByEndpoint, newSnap.Endpoint)
			if oldEntry.snap.unchanged(newSnap) {
				newServers = append(newServers, oldEntry)
				continue
			}
			stop(oldEntry.srv)
		}

		srv, lis, err := build(newSnap)
		if err != nil {
			continue
		}
		newServers = append(newServers, cfgGRPCServer{snap: newSnap, srv: srv, lis: lis})
		freshServers = append(freshServers, srv)
		freshListeners = append(freshListeners, lis)
	}

	// endpoints removed from the config entirely
	for _, entry := range oldByEndpoint {
		stop(entry.srv)
	}

	return
}

// reloadGRPC performs a fine-grained reload: only gRPC servers whose
// configuration or TLS certificate has actually changed are stopped and
// re-created; the rest continue serving without interruption.
func reloadGRPC(c *cfg) error {
	newCfg := writeGRPCConfig(c.appCfg)

	maxRecvMsgSizeOpt, err := getMaxRecvMsgSizeOpt(c)
	if err != nil {
		return fmt.Errorf("get max recv msg size option: %w", err)
	}

	c.cfgGRPC.mu.Lock()
	defer c.cfgGRPC.mu.Unlock()

	// The "old" state is each currently running server's own stored snapshot,
	// never a config snapshot recomputed separately and paired up by
	// position: that pairing silently breaks as soon as a previous reload
	// has ever built fewer servers than it was given endpoints for (e.g. one
	// endpoint failed to bind), since the servers slice then has fewer
	// entries than any freshly recomputed config snapshot, and matching them
	// up by index mismatches every entry from that point on.
	oldCfg := make(grpcConfigSnapshot, len(c.cfgGRPC.servers))
	for i, entry := range c.cfgGRPC.servers {
		oldCfg[i] = entry.snap
	}
	if selfSignedTLSConfigChanged(oldCfg, newCfg) {
		return errors.New("changing self-signed TLS endpoints requires node restart")
	}

	newServers, freshServers, freshListeners := reconcileGRPCServers(c.cfgGRPC.servers, newCfg,
		func(snap grpcServerSnapshot) (*grpc.Server, net.Listener, error) {
			srv, lis, err := buildSingleGRPCServer(c, snap.GRPC, maxRecvMsgSizeOpt)
			if err != nil {
				c.log.Error("failed to start gRPC server",
					zap.String("endpoint", snap.Endpoint), zap.Error(err))
			}
			return srv, lis, err
		},
		func(srv *grpc.Server) { stopGRPC("NeoFS Public API", srv, c.log) },
	)

	if len(newServers) == 0 {
		return errors.New("could not listen to any gRPC endpoints")
	}

	c.cfgGRPC.servers = newServers

	for _, reg := range c.cfgGRPC.serviceRegistrators {
		for _, srv := range freshServers {
			reg(srv)
		}
	}
	serveGRPCList(c, freshServers, freshListeners)
	return nil
}

func serveGRPC(c *cfg) {
	servers := make([]*grpc.Server, len(c.cfgGRPC.servers))
	listeners := make([]net.Listener, len(c.cfgGRPC.servers))
	for i, entry := range c.cfgGRPC.servers {
		servers[i] = entry.srv
		listeners[i] = entry.lis
	}
	serveGRPCList(c, servers, listeners)
}

func serveGRPCList(c *cfg, servers []*grpc.Server, listeners []net.Listener) {
	for i := range servers {
		srv := servers[i]
		lis := listeners[i]

		c.wg.Go(func() {
			defer func() {
				c.log.Info("stop listening gRPC endpoint",
					zap.Stringer("endpoint", lis.Addr()),
				)
			}()

			c.log.Info("start listening gRPC endpoint",
				zap.Stringer("endpoint", lis.Addr()),
			)

			if err := srv.Serve(lis); err != nil {
				c.log.Error("gRPC server failed", zap.Stringer("endpoint", lis.Addr()), zap.Error(err))
			}
		})
	}
}

func stopGRPC(name string, s *grpc.Server, l *zap.Logger) {
	l = l.With(zap.String("name", name))

	l.Info("stopping gRPC server...")

	// GracefulStop() may freeze forever, see #1270
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Minute):
		l.Info("gRPC cannot shutdown gracefully, forcing stop")
		s.Stop()
	}

	l.Info("gRPC server stopped successfully")
}
