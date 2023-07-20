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
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

type idsErr struct {
	ids []oid.ID
	err error
}

type testStorage struct {
	items map[string]idsErr
}

type testTraverserGenerator struct {
	c container.Container
	b map[uint64]placement.Builder
}

type testPlacementBuilder struct {
	vectors map[string][][]netmap.NodeInfo
}

type testClientCache struct {
	clients map[string]*testStorage
}

type simpleIDWriter struct {
	ids []oid.ID
}

type testEpochReceiver uint64

func (e testEpochReceiver) currentEpoch() (uint64, error) {
	return uint64(e), nil
}

func (s *simpleIDWriter) WriteIDs(ids []oid.ID) error {
	s.ids = append(s.ids, ids...)
	return nil
}

func newTestStorage() *testStorage {
	return &testStorage{
		items: make(map[string]idsErr),
	}
}

func (g *testTraverserGenerator) generateTraverser(_ cid.ID, epoch uint64) (*placement.Traverser, error) {
	return placement.NewTraverser(
		placement.ForContainer(g.c),
		placement.UseBuilder(g.b[epoch]),
		placement.WithoutSuccessTracking(),
	)
}

func (p *testPlacementBuilder) BuildPlacement(cnr cid.ID, obj *oid.ID, _ netmap.PlacementPolicy) ([][]netmap.NodeInfo, error) {
	var addr oid.Address
	addr.SetContainer(cnr)

	if obj != nil {
		addr.SetObject(*obj)
	}

	vs, ok := p.vectors[addr.EncodeToString()]
	if !ok {
		return nil, errors.New("vectors for address not found")
	}

	res := make([][]netmap.NodeInfo, len(vs))
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

func (s *testStorage) search(exec *execCtx) ([]oid.ID, error) {
	v, ok := s.items[exec.containerID().EncodeToString()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (c *testStorage) searchObjects(exec *execCtx, _ clientcore.NodeInfo) ([]oid.ID, error) {
	v, ok := c.items[exec.containerID().EncodeToString()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (c *testStorage) addResult(addr cid.ID, ids []oid.ID, err error) {
	c.items[addr.EncodeToString()] = idsErr{
		ids: ids,
		err: err,
	}
}

func testSHA256() (cs [sha256.Size]byte) {
	rand.Read(cs[:])
	return cs
}

func generateIDs(num int) []oid.ID {
	res := make([]oid.ID, num)

	for i := 0; i < num; i++ {
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

	newPrm := func(cnr cid.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(cnr)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		cnr := cidtest.ID()
		ids := generateIDs(10)
		storage.addResult(cnr, ids, nil)

		w := new(simpleIDWriter)
		p := newPrm(cnr, w)

		err := svc.Search(ctx, p)
		require.NoError(t, err)
		require.Equal(t, ids, w.ids)
	})

	t.Run("FAIL", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		cnr := cidtest.ID()
		testErr := errors.New("any error")
		storage.addResult(cnr, nil, testErr)

		w := new(simpleIDWriter)
		p := newPrm(cnr, w)

		err := svc.Search(ctx, p)
		require.ErrorIs(t, err, testErr)
	})
}

func testNodeMatrix(t testing.TB, dim []int) ([][]netmap.NodeInfo, [][]string) {
	mNodes := make([][]netmap.NodeInfo, len(dim))
	mAddr := make([][]string, len(dim))

	for i := range dim {
		ns := make([]netmap.NodeInfo, dim[i])
		as := make([]string, dim[i])

		for j := 0; j < dim[i]; j++ {
			a := fmt.Sprintf("/ip4/192.168.0.%s/tcp/%s",
				strconv.Itoa(i),
				strconv.Itoa(60000+j),
			)

			var ni netmap.NodeInfo
			ni.SetNetworkEndpoints(a)

			var na network.AddressGroup

			err := na.FromIterator(netmapcore.Node(ni))
			require.NoError(t, err)

			as[j] = network.StringifyGroup(na)

			ns[j] = ni
		}

		mNodes[i] = ns
		mAddr[i] = as
	}

	return mNodes, mAddr
}

func TestGetRemoteSmall(t *testing.T) {
	ctx := context.Background()

	placementDim := []int{2}

	rs := make([]netmap.ReplicaDescriptor, len(placementDim))
	for i := range placementDim {
		rs[i].SetNumberOfObjects(uint32(placementDim[i]))
	}

	var pp netmap.PlacementPolicy
	pp.AddReplicas(rs...)

	var cnr container.Container
	cnr.SetPlacementPolicy(pp)

	var id cid.ID
	cnr.CalculateID(&id)

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

	newPrm := func(id cid.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(id)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		var addr oid.Address
		addr.SetContainer(id)

		ns, as := testNodeMatrix(t, placementDim)

		builder := &testPlacementBuilder{
			vectors: map[string][][]netmap.NodeInfo{
				addr.EncodeToString(): ns,
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

	rs := make([]netmap.ReplicaDescriptor, len(placementDim))

	for i := range placementDim {
		rs[i].SetNumberOfObjects(uint32(placementDim[i]))
	}

	var pp netmap.PlacementPolicy
	pp.AddReplicas(rs...)

	var cnr container.Container
	cnr.SetPlacementPolicy(pp)

	var idCnr cid.ID
	cnr.CalculateID(&idCnr)

	var addr oid.Address
	addr.SetContainer(idCnr)

	ns, as := testNodeMatrix(t, placementDim)

	c11 := newTestStorage()
	ids11 := generateIDs(10)
	c11.addResult(idCnr, ids11, nil)

	c12 := newTestStorage()
	ids12 := generateIDs(10)
	c12.addResult(idCnr, ids12, nil)

	c21 := newTestStorage()
	ids21 := generateIDs(10)
	c21.addResult(idCnr, ids21, nil)

	c22 := newTestStorage()
	ids22 := generateIDs(10)
	c22.addResult(idCnr, ids22, nil)

	svc := &Service{cfg: new(cfg)}
	svc.log = test.NewLogger(false)
	svc.localStorage = newTestStorage()

	const curEpoch = 13

	svc.traverserGenerator = &testTraverserGenerator{
		c: cnr,
		b: map[uint64]placement.Builder{
			curEpoch: &testPlacementBuilder{
				vectors: map[string][][]netmap.NodeInfo{
					addr.EncodeToString(): ns[:1],
				},
			},
			curEpoch - 1: &testPlacementBuilder{
				vectors: map[string][][]netmap.NodeInfo{
					addr.EncodeToString(): ns[1:],
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
	p.WithContainerID(idCnr)
	p.SetWriter(w)

	commonPrm := new(util.CommonPrm)
	p.SetCommonParameters(commonPrm)

	assertContains := func(idsList ...[]oid.ID) {
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
