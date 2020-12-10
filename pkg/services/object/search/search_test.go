package searchsvc

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/pkg/errors"
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
	b placement.Builder
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

func (s *simpleIDWriter) WriteIDs(ids []*objectSDK.ID) error {
	s.ids = append(s.ids, ids...)
	return nil
}

func newTestStorage() *testStorage {
	return &testStorage{
		items: make(map[string]idsErr),
	}
}

func (g *testTraverserGenerator) generateTraverser(_ *container.ID) (*placement.Traverser, error) {
	return placement.NewTraverser(
		placement.ForContainer(g.c),
		placement.UseBuilder(g.b),
		placement.WithoutSuccessTracking(),
	)
}

func (p *testPlacementBuilder) BuildPlacement(addr *objectSDK.Address, _ *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	vs, ok := p.vectors[addr.String()]
	if !ok {
		return nil, errors.New("vectors for address not found")
	}

	return vs, nil
}

func (c *testClientCache) get(_ *ecdsa.PrivateKey, addr string) (searchClient, error) {
	v, ok := c.clients[addr]
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

func (c *testStorage) searchObjects(exec *execCtx) ([]*objectSDK.ID, error) {
	v, ok := c.items[exec.containerID().String()]
	if !ok {
		return nil, nil
	}

	return v.ids, v.err
}

func (c *testStorage) addResult(addr *container.ID, ids []*objectSDK.ID, err error) {
	c.items[addr.String()] = idsErr{
		ids: ids,
		err: err,
	}
}

func testSHA256() (cs [sha256.Size]byte) {
	rand.Read(cs[:])
	return cs
}

func generateCID() *container.ID {
	cid := container.NewID()
	cid.SetSHA256(testSHA256())

	return cid
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

	newPrm := func(cid *container.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(cid)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		cid := generateCID()
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

		cid := generateCID()
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

			var err error
			na, err := network.AddressFromString(a)
			require.NoError(t, err)

			as[j], err = na.IPAddrString()
			require.NoError(t, err)

			ni := netmap.NewNodeInfo()
			ni.SetAddress(a)

			ns[j] = *ni
		}

		mNodes[i] = netmap.NodesFromInfo(ns)
		mAddr[i] = as
	}

	return mNodes, mAddr
}

//
// func generateChain(ln int, cid *container.ID) ([]*object.RawObject, []*objectSDK.ID, []byte) {
// 	curID := generateID()
// 	var prevID *objectSDK.ID
//
// 	addr := objectSDK.NewAddress()
// 	addr.SetContainerID(cid)
//
// 	res := make([]*object.RawObject, 0, ln)
// 	ids := make([]*objectSDK.ID, 0, ln)
// 	payload := make([]byte, 0, ln*10)
//
// 	for i := 0; i < ln; i++ {
// 		ids = append(ids, curID)
// 		addr.SetObjectID(curID)
//
// 		payloadPart := make([]byte, 10)
// 		rand.Read(payloadPart)
//
// 		o := generateObject(addr, prevID, []byte{byte(i)})
// 		o.SetPayload(payloadPart)
// 		o.SetPayloadSize(uint64(len(payloadPart)))
// 		o.SetID(curID)
//
// 		payload = append(payload, payloadPart...)
//
// 		res = append(res, o)
//
// 		prevID = curID
// 		curID = generateID()
// 	}
//
// 	return res, ids, payload
// }

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
	cid := container.CalculateID(cnr)

	newSvc := func(b *testPlacementBuilder, c *testClientCache) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = newTestStorage()

		svc.traverserGenerator = &testTraverserGenerator{
			c: cnr,
			b: b,
		}
		svc.clientCache = c

		return svc
	}

	newPrm := func(cid *container.ID, w IDListWriter) Prm {
		p := Prm{}
		p.WithContainerID(cid)
		p.SetWriter(w)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		addr := objectSDK.NewAddress()
		addr.SetContainerID(cid)

		ns, as := testNodeMatrix(t, placementDim)

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.String(): ns,
			},
		}

		c1 := newTestStorage()
		ids1 := generateIDs(10)
		c1.addResult(cid, ids1, nil)

		c2 := newTestStorage()
		ids2 := generateIDs(10)
		c2.addResult(cid, ids2, nil)

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testStorage{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		w := new(simpleIDWriter)

		p := newPrm(cid, w)

		err := svc.Search(ctx, p)
		require.NoError(t, err)
		require.Len(t, w.ids, len(ids1)+len(ids2))

		for _, id := range append(ids1, ids2...) {
			require.Contains(t, w.ids, id)
		}
	})
}
