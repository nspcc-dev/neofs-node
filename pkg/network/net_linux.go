//go:build linux

package network

import (
	"syscall"

	"golang.org/x/sys/unix"
)

const tcpBufSize = 4 * 1024 * 1024

// TuneTCPConn sets some TCP options.
func TuneTCPConn(network, address string, c syscall.RawConn) error {
	var err error

	errControl := c.Control(func(fd uintptr) {
		var sock = int(fd)

		err = unix.SetsockoptInt(sock, unix.SOL_SOCKET, unix.SO_SNDBUF, tcpBufSize)
		if err != nil {
			return
		}
		err = unix.SetsockoptInt(sock, unix.SOL_SOCKET, unix.SO_RCVBUF, tcpBufSize)
		if err != nil {
			return
		}
	})
	if errControl != nil {
		return errControl
	}
	return err
}
