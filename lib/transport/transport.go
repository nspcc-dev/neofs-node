package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	circuit "github.com/rubyist/circuitbreaker"
)

type (
	// Transport is an interface of network connection listener.
	Transport interface {
		Dial(context.Context, multiaddr.Multiaddr, bool) (Connection, error)
		Listen(multiaddr.Multiaddr) (manet.Listener, error)
	}

	transport struct {
		threshold int64
		timeout   time.Duration
		panel     *circuit.Panel
	}
)

const defaultBreakerName = "_NeoFS"

func (t *transport) Dial(ctx context.Context, addr multiaddr.Multiaddr, reset bool) (Connection, error) {
	var (
		con     manet.Conn
		breaker = t.breakerLookup(addr)
	)

	if reset {
		breaker.Reset()
	}

	err := breaker.CallContext(ctx, func() (errCall error) {
		var d manet.Dialer
		con, errCall = d.DialContext(ctx, addr)
		return errCall
	}, t.timeout)

	if err != nil {
		return nil, err
	}

	return newConnection(con), nil
}

func (t *transport) Listen(addr multiaddr.Multiaddr) (manet.Listener, error) {
	return manet.Listen(addr)
}

func (t *transport) breakerLookup(addr fmt.Stringer) *circuit.Breaker {
	panel := defaultBreakerName + addr.String()

	cb, ok := t.panel.Get(panel)
	if !ok {
		cb = circuit.NewConsecutiveBreaker(t.threshold)
		t.panel.Add(panel, cb)
	}

	return cb
}

// New is a transport component's constructor.
func New(threshold int64, timeout time.Duration) Transport {
	breaker := circuit.NewConsecutiveBreaker(threshold)

	panel := circuit.NewPanel()
	panel.Add(defaultBreakerName, breaker)

	return &transport{panel: panel, threshold: threshold, timeout: timeout}
}
