package main

import (
	"errors"
	"net"
	"testing"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func snap(endpoint string) grpcServerSnapshot {
	return grpcServerSnapshot{GRPC: grpcconfig.GRPC{Endpoint: endpoint}}
}

// A reload where one endpoint fails to build leaves fewer running servers
// than the config declares. A later reload must still match each server to
// its own endpoint, which pairing by slice position does not.
func TestReconcileGRPCServers_SurvivesPriorPartialFailure(t *testing.T) {
	srvA := grpc.NewServer()
	srvB := grpc.NewServer()
	srvC := grpc.NewServer()

	old := []cfgGRPCServer{
		{snap: snap("A"), srv: srvA, lis: nil},
		{snap: snap("B"), srv: srvB, lis: nil},
		{snap: snap("C"), srv: srvC, lis: nil},
	}

	// Reload #1: B's config changed, forcing a rebuild, but the rebuild
	// fails (e.g. the new address is temporarily unbindable). A and C are
	// unchanged.
	bChanged := snap("B")
	bChanged.ConnLimit = 1 // any field unchanged() compares
	newCfg1 := grpcConfigSnapshot{snap("A"), bChanged, snap("C")}

	var stopped1 []*grpc.Server
	newServers1, fresh1, freshLis1 := reconcileGRPCServers(old, newCfg1,
		func(s grpcServerSnapshot) (*grpc.Server, net.Listener, error) {
			if s.Endpoint == "B" {
				return nil, nil, errors.New("bind: address already in use")
			}
			t.Fatalf("build should not be called for unchanged endpoint %q", s.Endpoint)
			return nil, nil, nil
		},
		func(srv *grpc.Server) { stopped1 = append(stopped1, srv) },
	)

	require.Empty(t, fresh1)
	require.Empty(t, freshLis1)
	require.Equal(t, []*grpc.Server{srvB}, stopped1, "B's old server must be stopped even though its rebuild failed")
	require.Len(t, newServers1, 2, "B must be dropped after its failed rebuild")
	byEndpoint1 := make(map[string]cfgGRPCServer, len(newServers1))
	for _, entry := range newServers1 {
		byEndpoint1[entry.snap.Endpoint] = entry
	}
	require.Same(t, srvA, byEndpoint1["A"].srv)
	require.Same(t, srvC, byEndpoint1["C"].srv)
	require.NotContains(t, byEndpoint1, "B")

	// Reload #2: the same config again, so nothing has changed further - B
	// is simply absent from what's actually running (newServers1, now only 2
	// entries), while the declared config still has all 3 endpoints. This is
	// exactly the "N declared, N-1 running" state the finding is about.
	newCfg2 := newCfg1

	var built2 []string
	srvB2 := grpc.NewServer()
	newServers2, fresh2, _ := reconcileGRPCServers(newServers1, newCfg2,
		func(s grpcServerSnapshot) (*grpc.Server, net.Listener, error) {
			built2 = append(built2, s.Endpoint)
			if s.Endpoint == "B" {
				return srvB2, nil, nil
			}
			t.Fatalf("build should not be called for unchanged endpoint %q", s.Endpoint)
			return nil, nil, nil
		},
		func(*grpc.Server) { t.Fatal("no old server should be stopped on this reload") },
	)

	require.Equal(t, []string{"B"}, built2, "only B should be (re)built - A and C are unchanged and still running")
	require.Equal(t, []*grpc.Server{srvB2}, fresh2)
	require.Len(t, newServers2, 3)
	byEndpoint2 := make(map[string]cfgGRPCServer, len(newServers2))
	for _, entry := range newServers2 {
		byEndpoint2[entry.snap.Endpoint] = entry
	}
	require.Same(t, srvA, byEndpoint2["A"].srv, "A must still be its own original server")
	require.Same(t, srvC, byEndpoint2["C"].srv, "C must still be its own original server, not misattributed to B")
	require.Same(t, srvB2, byEndpoint2["B"].srv)
}
