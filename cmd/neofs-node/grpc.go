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
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
		s.TLS.Key == other.TLS.Key &&
		s.certFingerprint == other.certFingerprint
}

type grpcConfigSnapshot []grpcServerSnapshot

func writeGRPCConfig(c *config.Config) grpcConfigSnapshot {
	snap := make(grpcConfigSnapshot, len(c.GRPC))
	for i, sc := range c.GRPC {
		snap[i] = grpcServerSnapshot{
			GRPC:            sc,
			certFingerprint: tlsCertFingerprint(sc.TLS.Certificate, sc.TLS.Key),
		}
	}
	return snap
}

func tlsCertFingerprint(certFile, keyFile string) string {
	if keyFile == "" {
		return ""
	}

	h := sha256.New()
	err := hashFileLimited(h, certFile, maxTLSFingerprintFileBytes)
	if err != nil {
		return ""
	}
	_, _ = h.Write([]byte{0})
	err = hashFileLimited(h, keyFile, maxTLSFingerprintFileBytes)
	if err != nil {
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
		copy(srvs, c.cfgGRPC.servers)
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
	for _, sc := range c.appCfg.GRPC {
		srv, lis, err := buildSingleGRPCServer(c, sc, maxRecvMsgSizeOpt)
		if err != nil {
			return err
		}
		c.cfgGRPC.listeners = append(c.cfgGRPC.listeners, lis)
		c.cfgGRPC.servers = append(c.cfgGRPC.servers, srv)
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

	if tlsCfg.Key != "" {
		certFile, keyFile := tlsCfg.Certificate, tlsCfg.Key

		if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
			c.log.Error("could not read certificate from file", zap.Error(err))
			return nil, nil, err
		}

		// read certificate from disk on each handshake to pick up renewals automatically.
		creds := credentials.NewTLS(&tls.Config{
			GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
				cert, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					return nil, fmt.Errorf("reload TLS certificate: %w", err)
				}
				return &tls.Config{
					Certificates: []tls.Certificate{cert},
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

// reloadGRPC performs a fine-grained reload: only gRPC servers whose
// configuration or TLS certificate has actually changed are stopped and
// re-created; the rest continue serving without interruption.
func reloadGRPC(c *cfg, oldCfg grpcConfigSnapshot) error {
	newCfg := writeGRPCConfig(c.appCfg)

	maxRecvMsgSizeOpt, err := getMaxRecvMsgSizeOpt(c)
	if err != nil {
		return fmt.Errorf("get max recv msg size option: %w", err)
	}

	c.cfgGRPC.mu.Lock()
	defer c.cfgGRPC.mu.Unlock()

	type serverEntry struct {
		srv  *grpc.Server
		lis  net.Listener
		snap grpcServerSnapshot
	}
	oldByEndpoint := make(map[string]serverEntry, len(c.cfgGRPC.servers))
	for i, srv := range c.cfgGRPC.servers {
		if i < len(oldCfg) {
			oldByEndpoint[oldCfg[i].Endpoint] = serverEntry{
				srv:  srv,
				lis:  c.cfgGRPC.listeners[i],
				snap: oldCfg[i],
			}
		}
	}

	newServers := make([]*grpc.Server, 0, len(newCfg))
	newListeners := make([]net.Listener, 0, len(newCfg))
	// freshServers/freshListeners hold only newly created servers that need
	// service registration and must start serving.
	var freshServers []*grpc.Server
	var freshListeners []net.Listener

	for _, newSnap := range newCfg {
		if old, ok := oldByEndpoint[newSnap.Endpoint]; ok {
			delete(oldByEndpoint, newSnap.Endpoint)
			if old.snap.unchanged(newSnap) {
				newServers = append(newServers, old.srv)
				newListeners = append(newListeners, old.lis)
				continue
			}
			stopGRPC("NeoFS Public API", old.srv, c.log)
		}

		srv, lis, err := buildSingleGRPCServer(c, newSnap.GRPC, maxRecvMsgSizeOpt)
		if err != nil {
			c.log.Error("failed to start gRPC server",
				zap.String("endpoint", newSnap.Endpoint), zap.Error(err))
			continue
		}
		newServers = append(newServers, srv)
		newListeners = append(newListeners, lis)
		freshServers = append(freshServers, srv)
		freshListeners = append(freshListeners, lis)
	}

	// stop servers that were removed from the config entirely
	for _, entry := range oldByEndpoint {
		stopGRPC("NeoFS Public API", entry.srv, c.log)
	}

	if len(newServers) == 0 {
		return errors.New("could not listen to any gRPC endpoints")
	}

	c.cfgGRPC.servers = newServers
	c.cfgGRPC.listeners = newListeners

	for _, reg := range c.cfgGRPC.serviceRegistrators {
		for _, srv := range freshServers {
			reg(srv)
		}
	}
	serveGRPCList(c, freshServers, freshListeners)
	return nil
}

func serveGRPC(c *cfg) {
	serveGRPCList(c, c.cfgGRPC.servers, c.cfgGRPC.listeners)
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
