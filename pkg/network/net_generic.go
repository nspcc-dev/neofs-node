//go:build !linux

package network

import (
	"syscall"
)

// TuneTCPConn does nothing.
func TuneTCPConn(network, address string, c syscall.RawConn) error {
	return nil
}
