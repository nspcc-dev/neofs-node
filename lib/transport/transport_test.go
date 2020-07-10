package transport

import (
	"context"
	"net"
	"testing"
	"time"

	manet "github.com/multiformats/go-multiaddr-net"
	circuit "github.com/rubyist/circuitbreaker"
	"github.com/stretchr/testify/require"
)

func TestTransport(t *testing.T) {
	var (
		attempts    = int64(5)
		lc          net.ListenConfig
		tr          = New(attempts, time.Second)
		ctx, cancel = context.WithCancel(context.TODO())
	)

	defer cancel()

	lis1, err := lc.Listen(ctx, "tcp", ":0")
	require.NoError(t, err)

	addr1, err := manet.FromNetAddr(lis1.Addr())
	require.NoError(t, err)

	_, err = tr.Dial(ctx, addr1, false)
	require.NoError(t, err)

	lis2, err := lc.Listen(ctx, "tcp", ":0")
	require.NoError(t, err)

	addr2, err := manet.FromNetAddr(lis2.Addr())
	require.NoError(t, err)

	_, err = tr.Dial(ctx, addr1, false)
	require.NoError(t, err)

	require.NoError(t, lis1.Close())

	for i := int64(0); i < 10; i++ {
		_, err = tr.Dial(ctx, addr1, false)
		require.Error(t, err)

		if i >= attempts {
			require.EqualError(t, err, circuit.ErrBreakerOpen.Error())
		}

		_, err = tr.Dial(ctx, addr2, false)
		require.NoError(t, err)
	}

	time.Sleep(time.Second)

	_, err = tr.Dial(ctx, addr1, false)
	require.Error(t, err)
	require.NotContains(t, err.Error(), circuit.ErrBreakerOpen.Error())
}
