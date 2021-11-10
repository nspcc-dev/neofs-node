package searchsvc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"testing"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

type idsErr struct {
	ids []*objectSDK.ID
	err error
}

type testStorage struct {
	items map[string]idsErr
}

type testTraverserGenerator struct {
	c *container.Container
	b map[uint64]placement.Builder
}

type testPlacementBuilder struct {
	vectors map[string][]netmap.Nodes
}

type testClientCache struct {
	clients map[string]*testStorage
}

type simpleIDWriter struct {
	ids []*objectSDK.ID
}

type testEpochReceiver uint64

func (e testEpochReceiver) currentEpoch() (uint64, error) {
	return uint64(e), nil
}

func (s *simpleIDWriter) WriteIDs(ids []*objectSDK.ID) error {
	s.ids = append(s.ids, ids...)
	return nil
}

func newTestStorage() *testStorage {
	return &testStorage{
		items: make(map[string]idsErr),
	}
}

func (g *testTraverserGenerator) generateTraverser(_ *cid.ID, epoch uint64) (*placement.Traverser, error) {
	return placement.NewTraverser(
		placement.ForContainer(g.c),
		placement.UseBuilder(g.b[epoch]),
		placement.WithoutSuccessTracking(),
	)
}

func (p *testPlacementBuilder) BuildPlacement(addr *objectSDK.Address, _ *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	vs, ok := p.vectors[addr.String()]
	if !ok {
		return nil, errors.New("vectors for address not found")
	}

	res := make([]netmap.Nodes, len(vs))
	copy(res, vs)

	return res, nil
}

func (c *testClientCache) get(info clientcore.NodeInfo) (searchClient, error) {
	v, ok := c.clients[network.StringifyGroup(info.AddressGroup())]
	if !ok {
		return nil, errors.New("could not construct client")
	}

	return v, nil
}

func (s *testStorage) search(exec *execCtx) ([]*objectSDK.ID, error) {
	v, ok := s.items[exec.containerID().String()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (c *testStorage) searchObjects(exec *execCtx, _ clientcore.NodeInfo) ([]*objectSDK.ID, error) {
	v, ok := c.items[exec.containerID().String()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (c *testStorage) addResult(addr *cid.ID, ids []*objectSDK.ID, err error) {
	c.items[addr.String()] = idsErr{
		ids: ids,
		err: err,
	}
}

func testSHA256() (cs [sha256.Size]byte) {
	rand.Read(cs[:])
	return cs
}

func generateIDs(num int) []*objectSDK.ID {
	res := make([]*objectSDK.ID, num)

	for i := 0; i < num; i++ {
		res[i] = objectSDK.NewID()
		res[i].SetSHA256(testSHA256())
	}

	return res
}

func TestGetLocalOnly(t *testing.T) {
	ctx := context.Background()

	newSvc := func(storage *testStorage) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = storage

		return svc
	}

	newPrm := func(cid *cid.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(cid)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		cid := cidtest.GenerateID()
		ids := generateIDs(10)
		storage.addResult(cid, ids, nil)

		w := new(simpleIDWriter)
		p := newPrm(cid, w)

		err := svc.Search(ctx, p)
		require.NoError(t, err)
		require.Equal(t, ids, w.ids)
	})

	t.Run("FAIL", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		cid := cidtest.GenerateID()
		testErr := errors.New("any error")
		storage.addResult(cid, nil, testErr)

		w := new(simpleIDWriter)
		p := newPrm(cid, w)

		err := svc.Search(ctx, p)
		require.True(t, errors.Is(err, testErr))
	})
}

func testNodeMatrix(t testing.TB, dim []int) ([]netmap.Nodes, [][]string) {
	mNodes := make([]netmap.Nodes, len(dim))
	mAddr := make([][]string, len(dim))

	for i := range dim {
		ns := make([]netmap.NodeInfo, dim[i])
		as := make([]string, dim[i])

		for j := 0; j < dim[i]; j++ {
			a := fmt.Sprintf("/ip4/192.168.0.%s/tcp/%s",
				strconv.Itoa(i),
				strconv.Itoa(60000+j),
			)

			ni := netmap.NewNodeInfo()
			ni.SetAddresses(a)

			var na network.AddressGroup

			err := na.FromIterator(ni)
			require.NoError(t, err)

			as[j] = network.StringifyGroup(na)

			ns[j] = *ni
		}

		mNodes[i] = netmap.NodesFromInfo(ns)
		mAddr[i] = as
	}

	return mNodes, mAddr
}

func TestGetRemoteSmall(t *testing.T) {
	ctx := context.Background()

	placementDim := []int{2}

	rs := make([]*netmap.Replica, 0, len(placementDim))
	for i := range placementDim {
		r := netmap.NewReplica()
		r.SetCount(uint32(placementDim[i]))

		rs = append(rs, r)
	}

	pp := netmap.NewPlacementPolicy()
	pp.SetReplicas(rs...)

	cnr := container.New(container.WithPolicy(pp))
	id := container.CalculateID(cnr)

	newSvc := func(b *testPlacementBuilder, c *testClientCache) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = newTestStorage()

		const curEpoch = 13

		svc.traverserGenerator = &testTraverserGenerator{
			c: cnr,
			b: map[uint64]placement.Builder{
				curEpoch: b,
			},
		}
		svc.clientConstructor = c
		svc.currentEpochReceiver = testEpochReceiver(curEpoch)

		return svc
	}

	newPrm := func(id *cid.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(id)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		addr := objectSDK.NewAddress()
		addr.SetContainerID(id)

		ns, as := testNodeMatrix(t, placementDim)

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.String(): ns,
			},
		}

		c1 := newTestStorage()
		ids1 := generateIDs(10)
		c1.addResult(id, ids1, nil)

		c2 := newTestStorage()
		ids2 := generateIDs(10)
		c2.addResult(id, ids2, nil)

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testStorage{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		w := new(simpleIDWriter)

		p := newPrm(id, w)

		err := svc.Search(ctx, p)
		require.NoError(t, err)
		require.Len(t, w.ids, len(ids1)+len(ids2))

		for _, id := range append(ids1, ids2...) {
			require.Contains(t, w.ids, id)
		}
	})
}

func TestGetFromPastEpoch(t *testing.T) {
	ctx := context.Background()

	placementDim := []int{2, 2}

	rs := make([]*netmap.Replica, 0, len(placementDim))

	for i := range placementDim {
		r := netmap.NewReplica()
		r.SetCount(uint32(placementDim[i]))

		rs = append(rs, r)
	}

	pp := netmap.NewPlacementPolicy()
	pp.SetReplicas(rs...)

	cnr := container.New(container.WithPolicy(pp))
	cid := container.CalculateID(cnr)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)

	ns, as := testNodeMatrix(t, placementDim)

	c11 := newTestStorage()
	ids11 := generateIDs(10)
	c11.addResult(cid, ids11, nil)

	c12 := newTestStorage()
	ids12 := generateIDs(10)
	c12.addResult(cid, ids12, nil)

	c21 := newTestStorage()
	ids21 := generateIDs(10)
	c21.addResult(cid, ids21, nil)

	c22 := newTestStorage()
	ids22 := generateIDs(10)
	c22.addResult(cid, ids22, nil)

	svc := &Service{cfg: new(cfg)}
	svc.log = test.NewLogger(false)
	svc.localStorage = newTestStorage()

	const curEpoch = 13

	svc.traverserGenerator = &testTraverserGenerator{
		c: cnr,
		b: map[uint64]placement.Builder{
			curEpoch: &testPlacementBuilder{
				vectors: map[string][]netmap.Nodes{
					addr.String(): ns[:1],
				},
			},
			curEpoch - 1: &testPlacementBuilder{
				vectors: map[string][]netmap.Nodes{
					addr.String(): ns[1:],
				},
			},
		},
	}

	svc.clientConstructor = &testClientCache{
		clients: map[string]*testStorage{
			as[0][0]: c11,
			as[0][1]: c12,
			as[1][0]: c21,
			as[1][1]: c22,
		},
	}

	svc.currentEpochReceiver = testEpochReceiver(curEpoch)

	w := new(simpleIDWriter)

	p := Prm{}
	p.WithContainerID(cid)
	p.SetWriter(w)

	commonPrm := new(util.CommonPrm)
	p.SetCommonParameters(commonPrm)

	assertContains := func(idsList ...[]*objectSDK.ID) {
		var sz int

		for _, ids := range idsList {
			sz += len(ids)

			for _, id := range ids {
				require.Contains(t, w.ids, id)
			}
		}

		require.Len(t, w.ids, sz)
	}

	err := svc.Search(ctx, p)
	require.NoError(t, err)
	assertContains(ids11, ids12)

	commonPrm.SetNetmapLookupDepth(1)
	w = new(simpleIDWriter)
	p.SetWriter(w)

	err = svc.Search(ctx, p)
	require.NoError(t, err)
	assertContains(ids11, ids12, ids21, ids22)
}
