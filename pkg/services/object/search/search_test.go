package searchsvc

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"testing"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type idsErr struct {
	ids []oid.ID
	err error
}

type testStorage struct {
	items map[string]idsErr
}

type testContainers struct {
	b *testPlacementBuilder
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

func (s *simpleIDWriter) WriteIDs(ids []oid.ID) error {
	s.ids = append(s.ids, ids...)
	return nil
}

func newTestStorage() *testStorage {
	return &testStorage{
		items: make(map[string]idsErr),
	}
}

func (g *testContainers) ForEachRemoteContainerNode(cnr cid.ID, f func(info netmap.NodeInfo)) error {
	var anyPolicy netmap.PlacementPolicy // policy is ignored in this test
	nodeSets, err := g.b.BuildPlacement(cnr, nil, anyPolicy)
	if err != nil {
		return err
	}

	for i := range nodeSets {
		for j := range nodeSets[i] {
			f(nodeSets[i][j])
		}
	}

	return nil
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

func (ts *testStorage) search(exec *execCtx) ([]oid.ID, error) {
	v, ok := ts.items[exec.containerID().EncodeToString()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (ts *testStorage) searchObjects(_ context.Context, exec *execCtx, _ clientcore.NodeInfo) ([]oid.ID, error) {
	v, ok := ts.items[exec.containerID().EncodeToString()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (ts *testStorage) addResult(addr cid.ID, ids []oid.ID, err error) {
	ts.items[addr.EncodeToString()] = idsErr{
		ids: ids,
		err: err,
	}
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
		ids := oidtest.IDs(10)
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

		for j := range dim[i] {
			a := fmt.Sprintf("/ip4/192.168.0.%s/tcp/%s",
				strconv.Itoa(i),
				strconv.Itoa(60000+j),
			)

			bPubKey := make([]byte, 33)
			_, _ = rand.Read(bPubKey)

			var ni netmap.NodeInfo
			ni.SetNetworkEndpoints(a)
			ni.SetPublicKey(bPubKey)

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

	id := cidtest.ID()

	newSvc := func(b *testPlacementBuilder, c *testClientCache) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = newTestStorage()

		svc.containers = &testContainers{
			b: b,
		}
		svc.clientConstructor = c

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
		ids1 := oidtest.IDs(10)
		c1.addResult(id, ids1, nil)

		c2 := newTestStorage()
		ids2 := oidtest.IDs(10)
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

func TestNumericFilters(t *testing.T) {
	var s Service
	ctx := context.Background()
	var prm Prm

	for _, op := range []objectsdk.SearchMatchType{} {
		var query objectsdk.SearchFilters
		query.AddFilter("any_key", "1.0", op)

		prm.WithSearchFilters(query)

		err := s.Search(ctx, prm)
		require.ErrorIs(t, err, object.ErrInvalidSearchQuery, op)
		require.ErrorContains(t, err, "numeric filter with non-decimal value", op)
	}
}
