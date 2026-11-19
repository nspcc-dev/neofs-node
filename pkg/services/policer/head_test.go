package policer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestHeadRemoteMetrics(t *testing.T) {
	var (
		node netmap.NodeInfo
		addr = oidtest.Address()
	)
	node.SetPublicKey([]byte("any"))

	for _, tc := range []struct {
		name string
		err  error
		res  HeadResult
	}{
		{name: "ok", res: HeadOK},
		{name: "not found", err: fmt.Errorf("wrapped: %w", apistatus.ErrObjectNotFound), res: HeadNotFound},
		{name: "maintenance", err: apistatus.ErrNodeUnderMaintenance, res: HeadMaintenance},
		{name: "timeout", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), res: HeadTimeout},
		{name: "already removed", err: apistatus.ErrObjectAlreadyRemoved, res: HeadError},
		{name: "other", err: errors.New("any"), res: HeadError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conns := newMockAPIConnections()
			conns.setHeadResult(node, addr, object.Object{}, tc.err)
			m := new(mockMetrics)
			p := &Policer{cfg: &cfg{metrics: m, apiConns: conns}}

			_, err := p.headRemote(context.Background(), node, addr, true, iec.PartInfo{RuleIndex: -1})
			require.ErrorIs(t, err, tc.err)

			for res := range m.heads {
				if HeadResult(res) == tc.res {
					require.EqualValues(t, 1, m.heads[res].Load())
				} else {
					require.Zero(t, m.heads[res].Load())
				}
			}
		})
	}

	t.Run("canceled is not reported", func(t *testing.T) {
		conns := newMockAPIConnections()
		conns.setHeadResult(node, addr, object.Object{}, context.Canceled)
		m := new(mockMetrics)
		p := &Policer{cfg: &cfg{metrics: m, apiConns: conns}}

		_, err := p.headRemote(context.Background(), node, addr, true, iec.PartInfo{RuleIndex: -1})
		require.ErrorIs(t, err, context.Canceled)
		for res := range m.heads {
			require.Zero(t, m.heads[res].Load())
		}
	})
}
