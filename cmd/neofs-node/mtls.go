package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// tlsRecordTypeHandshake is the first byte (ContentType) of a TLS handshake
// record.
const tlsRecordTypeHandshake = 0x16

// nodeAuthCreds are gRPC server transport credentials that serve regular clients
// and inter-node mTLS connections on the same port.
type nodeAuthCreds struct {
	tls   credentials.TransportCredentials
	plain credentials.TransportCredentials
}

// newNodeAuthCreds builds the shared-port credentials. adminCertFile and
// adminKeyFile are the optional server-side TLS certificate served to regular
// clients; when empty, only plain clients and inter-node mTLS are expected.
func newNodeAuthCreds(c *cfg, adminCertFile, adminKeyFile string) credentials.TransportCredentials {
	return &nodeAuthCreds{
		tls:   credentials.NewTLS(serverTLSConfig(c, adminCertFile, adminKeyFile)),
		plain: insecure.NewCredentials(),
	}
}

func (a *nodeAuthCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	first := make([]byte, 1)
	if _, err := io.ReadFull(raw, first); err != nil {
		return nil, nil, fmt.Errorf("peek first connection byte: %w", err)
	}
	conn := &prefixConn{Conn: raw, prefix: first}
	if first[0] == tlsRecordTypeHandshake {
		return a.tls.ServerHandshake(conn)
	}
	return a.plain.ServerHandshake(conn)
}

func (a *nodeAuthCreds) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return a.plain.ClientHandshake(ctx, authority, raw)
}

func (a *nodeAuthCreds) Info() credentials.ProtocolInfo { return a.tls.Info() }

func (a *nodeAuthCreds) Clone() credentials.TransportCredentials {
	return &nodeAuthCreds{tls: a.tls.Clone(), plain: a.plain.Clone()}
}

func (a *nodeAuthCreds) OverrideServerName(string) error { return nil }

// prefixConn is a net.Conn that replays a number of already-read bytes before
// continuing with the underlying connection. It lets the handshake sniffer put
// the peeked byte back so the chosen handshaker sees the full stream.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

func serverTLSConfig(c *cfg, adminCertFile, adminKeyFile string) *tls.Config {
	node := nodeServerTLSConfig(c)
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2"},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if hello.ServerName == peerauth.TLSServerName {
				return node, nil
			}
			if adminKeyFile == "" {
				return nil, fmt.Errorf("no server TLS certificate configured for client %q", hello.ServerName)
			}
			cert, err := tls.LoadX509KeyPair(adminCertFile, adminKeyFile)
			if err != nil {
				return nil, fmt.Errorf("reload TLS certificate: %w", err)
			}
			return &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
				NextProtos:   []string{"h2"},
			}, nil
		},
	}
}

// nodeServerTLSConfig builds the TLS sub-configuration used for inter-node mTLS
// connections. Only fellow storage nodes are expected here: the verifier
// rejects any peer whose key is not in the current network map.
func nodeServerTLSConfig(c *cfg) *tls.Config {
	return &tls.Config{
		Certificates:          []tls.Certificate{c.tlsCert},
		ClientAuth:            tls.RequireAnyClientCert,
		VerifyPeerCertificate: makeNetmapVerifier(c),
		MinVersion:            tls.VersionTLS12,
		NextProtos:            []string{"h2"},
	}
}

// makeNetmapVerifier returns a TLS VerifyPeerCertificate hook that accepts a
// client certificate only if its public key belongs to a node in the current
// network map.
func makeNetmapVerifier(c *cfg) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		pub, err := peerauth.PeerPubKey(rawCerts)
		if err != nil {
			return fmt.Errorf("extract peer pubkey: %w", err)
		}

		if !isNetmapNode(c, pub) {
			c.log.Warn("mTLS: rejecting inter-node peer absent from network map",
				zap.String("pubkey", hex.EncodeToString(pub)))
			return fmt.Errorf("peer key %s is not a network map node", hex.EncodeToString(pub))
		}

		c.log.Info("mTLS: peer authenticated as known SN",
			zap.String("pubkey", hex.EncodeToString(pub)))
		return nil
	}
}

// isNetmapNode reports whether pub is the public key of some node in the
// current network map snapshot.
func isNetmapNode(c *cfg, pub []byte) bool {
	val := c.netMap.Load()
	if val == nil {
		return false
	}
	nm, ok := val.(netmap.NetMap)
	if !ok {
		return false
	}
	for _, n := range nm.Nodes() {
		if bytes.Equal(n.PublicKey(), pub) {
			return true
		}
	}
	return false
}
