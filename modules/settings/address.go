package settings

import (
	"net"
	"strconv"
	"strings"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
)

const (
	protoTCP  = "tcp"
	protoUDP  = "udp"
	protoQUIC = "quic"
)

const emptyAddr = "0.0.0.0"

const ip4ColonCount = 1

var (
	errEmptyAddress     = internal.Error("`node.address` could not be empty")
	errEmptyProtocol    = internal.Error("`node.protocol` could not be empty")
	errUnknownProtocol  = internal.Error("`node.protocol` unknown protocol")
	errEmptyShutdownTTL = internal.Error("`node.shutdown_ttl` could not be empty")
)

func ipVersion(address string) string {
	if strings.Count(address, ":") > ip4ColonCount {
		return "ip6"
	}

	return "ip4"
}

func prepareAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", errors.Wrapf(err, "could not fetch host/port: %s", address)
	} else if host == "" {
		host = emptyAddr
	}

	addr, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return "", errors.Wrapf(err, "could not resolve address: %s:%s", host, port)
	}

	return net.JoinHostPort(addr.IP.String(), port), nil
}

func resolveAddress(proto, address string) (string, string, error) {
	var (
		ip         net.IP
		host, port string
	)

	switch proto {
	case protoTCP:
		addr, err := net.ResolveTCPAddr(protoTCP, address)
		if err != nil {
			return "", "", errors.Wrapf(err, "could not parse address: '%s'", address)
		}

		ip = addr.IP
		port = strconv.Itoa(addr.Port)
	case protoUDP, protoQUIC:
		addr, err := net.ResolveUDPAddr(protoUDP, address)
		if err != nil {
			return "", "", errors.Wrapf(err, "could not parse address: '%s'", address)
		}

		ip = addr.IP
		port = strconv.Itoa(addr.Port)
	default:
		return "", "", errors.Wrapf(errUnknownProtocol, "unknown protocol: '%s'", proto)
	}

	if host = ip.String(); ip == nil {
		host = emptyAddr
	}

	return host, port, nil
}

func multiAddressFromProtoAddress(proto, addr string) (multiaddr.Multiaddr, error) {
	var (
		err        error
		host, port string
		ipVer      = ipVersion(addr)
	)

	if host, port, err = resolveAddress(proto, addr); err != nil {
		return nil, errors.Wrapf(err, "could not resolve address: (%s) '%s'", proto, addr)
	}

	items := []string{
		ipVer,
		host,
		proto,
		port,
	}

	addr = "/" + strings.Join(items, "/")

	return multiaddr.NewMultiaddr(addr)
}
