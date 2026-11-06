package policer

import (
	"context"
	"errors"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	netmaptest "github.com/nspcc-dev/neofs-sdk-go/netmap/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPolicer_CheckReplication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		exp        string
		expTime    int64
		cleanupErr error
		modify     func(*object.Object)
		allow      bool
		deleted    bool
	}{
		{name: "expired epoch", exp: "9", deleted: true},
		{name: "current epoch needs no cleanup", exp: "10", cleanupErr: errors.New("must not clean up unexpired parts"), allow: true},
		{name: "no expiration", allow: true},
		{name: "invalid epoch", exp: "invalid"},
		{name: "expired time", expTime: 99, deleted: true},
		{name: "current second", expTime: 100, allow: true},
		{name: "conflicting expiration", exp: "9", expTime: 99},
		{name: "cleanup failure", exp: "9", cleanupErr: errors.New("storage failure")},
		{name: "standalone", exp: "9", modify: func(o *object.Object) { o.SetParent(nil) }, allow: true},
		{name: "complete parent", exp: "9", modify: func(o *object.Object) { o.SetParentID(oidtest.ID()) }, allow: true},
		{name: "subsequent part", exp: "9", modify: func(o *object.Object) { o.SetFirstID(oidtest.ID()) }, allow: true},
		{name: "link", exp: "9", modify: func(o *object.Object) { o.SetType(object.TypeLink) }, allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := objecttest.Object()
			parent.ResetRelations()
			parent.ResetID()
			parent.SetType(object.TypeRegular)
			parent.SetAttributes()
			if tc.exp != "" {
				parent.SetAttributes(object.NewAttribute(object.AttributeExpirationEpoch, tc.exp)) //nolint:staticcheck // Test legacy expiration.
			}
			if tc.expTime != 0 {
				require.NoError(t, parent.SetExpirationTime(time.Unix(tc.expTime, 0)))
			}
			part := objecttest.Object()
			part.ResetRelations()
			part.SetType(object.TypeRegular)
			part.SetParent(&parent)
			if tc.modify != nil {
				tc.modify(&part)
			}

			local := newTestLocalNode()
			local.expiredPartErr = tc.cleanupErr
			network := newMockNetwork()
			network.epoch = 10
			p := New(neofscryptotest.Signer(), WithNetwork(network), WithLogger(zap.NewNop()),
				WithTimeSource(func() time.Time { return time.Unix(100, 999) }))
			p.localStorage = local
			require.Equal(t, tc.allow, p.checkReplication(context.Background(), part.Address(), part.Marshal()))
			if tc.deleted {
				require.Equal(t, []oid.Address{part.Address()}, local.deletedObjects())
			} else {
				require.Empty(t, local.deletedObjects())
			}
		})
	}

	t.Run("malformed object", func(t *testing.T) {
		local := newTestLocalNode()
		p := New(neofscryptotest.Signer(), WithLogger(zap.NewNop()))
		p.localStorage = local
		require.False(t, p.checkReplication(context.Background(), oid.Address{}, []byte{0xff}))
		require.Empty(t, local.deletedObjects())
	})
}

// objectReplication runs the real pre-send check without a remote sender.
type objectReplication struct {
	obj   object.Object
	calls int
}

func (r *objectReplication) HandleTask(ctx context.Context, task replicator.Task, res replicator.TaskResult) {
	r.calls++
	task.SetObject(&r.obj)
	replicator.New(replicator.WithLogger(zap.NewNop())).HandleTask(ctx, task, res)
}

func TestPolicer_ExpiredFirstPartStopsPlacement(t *testing.T) {
	parent := objecttest.Object()
	parent.ResetRelations()
	parent.ResetID()
	parent.SetType(object.TypeRegular)
	parent.SetAttributes(object.NewAttribute(object.AttributeExpirationEpoch, "9")) //nolint:staticcheck // Test legacy expiration.
	part := objecttest.Object()
	part.ResetRelations()
	part.SetType(object.TypeRegular)
	part.SetParent(&parent)

	for _, tc := range []struct {
		name      string
		preserved bool
	}{
		{name: "deleted by storage"},
		{name: "preserved by storage", preserved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := newTestLocalNode()
			local.preserveExpiredPart = tc.preserved
			network := newMockNetwork()
			network.epoch = 10
			nodes := []netmap.NodeInfo{netmaptest.NodeInfo(), netmaptest.NodeInfo()}
			network.setObjectNodesRepResult(part.GetContainerID(), part.GetID(), [][]netmap.NodeInfo{{nodes[0]}, {nodes[1]}}, 1)
			conns := newMockAPIConnections()
			for _, node := range nodes {
				conns.setHeadResult(node, part.Address(), object.Object{}, apistatus.ErrObjectNotFound)
			}
			p := New(neofscryptotest.Signer(), WithNetwork(network), WithLogger(zap.NewNop()))
			p.localStorage = local
			p.apiConns = conns
			r := &objectReplication{obj: part}
			p.replicator = r
			p.processObject(context.Background(), objectcore.AddressWithAttributes{
				Address: part.Address(), Type: object.TypeRegular, Attributes: make([]string, 3),
			})
			require.Equal(t, 1, r.calls)
			if tc.preserved {
				require.Empty(t, local.deletedObjects())
			} else {
				require.Equal(t, []oid.Address{part.Address()}, local.deletedObjects())
			}
		})
	}
}

func TestNodeCache(t *testing.T) {
	cache := newNodeCache(&mockMetrics{})
	node := netmaptest.NodeInfo()

	require.Negative(t, cache.processStatus(node))

	cache.SubmitSuccessfulReplication(node)
	require.Zero(t, cache.processStatus(node))

	cache.submitReplicaCandidate(node)
	require.Positive(t, cache.processStatus(node))

	cache.submitReplicaHolder(node)
	require.Zero(t, cache.processStatus(node))
}
