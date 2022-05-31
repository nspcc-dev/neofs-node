package getsvc

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	inhumed map[string]struct{}

	virtual map[string]*objectSDK.SplitInfo

	phy map[string]*objectSDK.Object
}

type testTraverserGenerator struct {
	c *container.Container
	b map[uint64]placement.Builder
}

type testPlacementBuilder struct {
	vectors map[string][]netmap.Nodes
}

type testClientCache struct {
	clients map[string]*testClient
}

type testClient struct {
	results map[string]struct {
		obj *objectSDK.Object
		err error
	}
}

type testEpochReceiver uint64

func (e testEpochReceiver) currentEpoch() (uint64, error) {
	return uint64(e), nil
}

func newTestStorage() *testStorage {
	return &testStorage{
		inhumed: make(map[string]struct{}),
		virtual: make(map[string]*objectSDK.SplitInfo),
		phy:     make(map[string]*objectSDK.Object),
	}
}

func (g *testTraverserGenerator) GenerateTraverser(cnr cid.ID, obj *oid.ID, e uint64) (*placement.Traverser, error) {
	opts := make([]placement.Option, 3, 4)
	opts = append(opts,
		placement.ForContainer(g.c),
		placement.UseBuilder(g.b[e]),
		placement.SuccessAfter(1),
	)

	if obj != nil {
		opts = append(opts, placement.ForObject(*obj))
	}

	return placement.NewTraverser(opts...)
}

func (p *testPlacementBuilder) BuildPlacement(cnr cid.ID, obj *oid.ID, _ *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	var addr oid.Address
	addr.SetContainer(cnr)

	if obj != nil {
		addr.SetObject(*obj)
	}

	vs, ok := p.vectors[addr.EncodeToString()]
	if !ok {
		return nil, errors.New("vectors for address not found")
	}

	return vs, nil
}

func (c *testClientCache) get(info client.NodeInfo) (getClient, error) {
	v, ok := c.clients[network.StringifyGroup(info.AddressGroup())]
	if !ok {
		return nil, errors.New("could not construct client")
	}

	return v, nil
}

func newTestClient() *testClient {
	return &testClient{
		results: map[string]struct {
			obj *objectSDK.Object
			err error
		}{},
	}
}

func (c *testClient) getObject(exec *execCtx, _ client.NodeInfo) (*objectSDK.Object, error) {
	v, ok := c.results[exec.address().EncodeToString()]
	if !ok {
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	if v.err != nil {
		return nil, v.err
	}

	return cutToRange(v.obj, exec.ctxRange()), nil
}

func (c *testClient) addResult(addr oid.Address, obj *objectSDK.Object, err error) {
	c.results[addr.EncodeToString()] = struct {
		obj *objectSDK.Object
		err error
	}{obj: obj, err: err}
}

func (s *testStorage) get(exec *execCtx) (*objectSDK.Object, error) {
	var (
		ok    bool
		obj   *objectSDK.Object
		sAddr = exec.address().EncodeToString()
	)

	if _, ok = s.inhumed[sAddr]; ok {
		var errRemoved apistatus.ObjectAlreadyRemoved

		return nil, errRemoved
	}

	if info, ok := s.virtual[sAddr]; ok {
		return nil, objectSDK.NewSplitInfoError(info)
	}

	if obj, ok = s.phy[sAddr]; ok {
		return cutToRange(obj, exec.ctxRange()), nil
	}

	var errNotFound apistatus.ObjectNotFound

	return nil, errNotFound
}

func cutToRange(o *objectSDK.Object, rng *objectSDK.Range) *objectSDK.Object {
	if rng == nil {
		return o
	}

	from := rng.GetOffset()
	to := from + rng.GetLength()

	payload := o.Payload()

	o = o.CutPayload()
	o.SetPayload(payload[from:to])

	return o
}

func (s *testStorage) addPhy(addr oid.Address, obj *objectSDK.Object) {
	s.phy[addr.EncodeToString()] = obj
}

func (s *testStorage) addVirtual(addr oid.Address, info *objectSDK.SplitInfo) {
	s.virtual[addr.EncodeToString()] = info
}

func (s *testStorage) inhume(addr oid.Address) {
	s.inhumed[addr.EncodeToString()] = struct{}{}
}

func generateObject(addr oid.Address, prev *oid.ID, payload []byte, children ...oid.ID) *objectSDK.Object {
	obj := objectSDK.New()
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))
	if prev != nil {
		obj.SetPreviousID(*prev)
	}
	obj.SetChildren(children...)

	return obj
}

func TestGetLocalOnly(t *testing.T) {
	ctx := context.Background()

	newSvc := func(storage *testStorage) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = storage
		svc.assembly = true

		return svc
	}

	newPrm := func(raw bool, w ObjectWriter) Prm {
		p := Prm{}
		p.SetObjectWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		return p
	}

	newRngPrm := func(raw bool, w ChunkWriter, off, ln uint64) RangePrm {
		p := RangePrm{}
		p.SetChunkWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		r := objectSDK.NewRange()
		r.SetOffset(off)
		r.SetLength(ln)

		p.SetRange(r)

		return p
	}

	newHeadPrm := func(raw bool, w ObjectWriter) HeadPrm {
		p := HeadPrm{}
		p.SetHeaderWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(true)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		w := NewSimpleObjectWriter()
		p := newPrm(false, w)

		payloadSz := uint64(10)
		payload := make([]byte, payloadSz)
		rand.Read(payload)

		addr := oidtest.Address()
		obj := generateObject(addr, nil, payload)

		storage.addPhy(addr, obj)

		p.WithAddress(addr)

		storage.addPhy(addr, obj)

		err := svc.Get(ctx, p)

		require.NoError(t, err)

		require.Equal(t, obj, w.Object())

		w = NewSimpleObjectWriter()

		rngPrm := newRngPrm(false, w, payloadSz/3, payloadSz/3)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[payloadSz/3:2*payloadSz/3], w.Object().Payload())

		w = NewSimpleObjectWriter()
		headPrm := newHeadPrm(false, w)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), w.Object())
	})

	t.Run("INHUMED", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := oidtest.Address()

		storage.inhume(addr)

		p.WithAddress(addr)

		err := svc.Get(ctx, p)

		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
	})

	t.Run("404", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := oidtest.Address()

		p.WithAddress(addr)

		err := svc.Get(ctx, p)

		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)

		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("VIRTUAL", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(true, nil)

		addr := oidtest.Address()

		splitInfo := objectSDK.NewSplitInfo()
		splitInfo.SetSplitID(objectSDK.NewSplitID())
		splitInfo.SetLink(oidtest.ID())
		splitInfo.SetLastPart(oidtest.ID())

		p.WithAddress(addr)

		storage.addVirtual(addr, splitInfo)

		err := svc.Get(ctx, p)

		errSplit := objectSDK.NewSplitInfoError(objectSDK.NewSplitInfo())

		require.True(t, errors.As(err, &errSplit))

		require.Equal(t, splitInfo, errSplit.SplitInfo())

		rngPrm := newRngPrm(true, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.Get(ctx, p)

		require.True(t, errors.As(err, &errSplit))

		headPrm := newHeadPrm(true, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.True(t, errors.As(err, &errSplit))
		require.Equal(t, splitInfo, errSplit.SplitInfo())
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

func generateChain(ln int, cnr cid.ID) ([]*objectSDK.Object, []oid.ID, []byte) {
	curID := oidtest.ID()
	var prevID *oid.ID

	var addr oid.Address
	addr.SetContainer(cnr)

	res := make([]*objectSDK.Object, 0, ln)
	ids := make([]oid.ID, 0, ln)
	payload := make([]byte, 0, ln*10)

	for i := 0; i < ln; i++ {
		ids = append(ids, curID)
		addr.SetObject(curID)

		payloadPart := make([]byte, 10)
		rand.Read(payloadPart)

		o := generateObject(addr, prevID, []byte{byte(i)})
		o.SetPayload(payloadPart)
		o.SetPayloadSize(uint64(len(payloadPart)))
		o.SetID(curID)

		payload = append(payload, payloadPart...)

		res = append(res, o)

		cpCurID := curID
		prevID = &cpCurID
		curID = oidtest.ID()
	}

	return res, ids, payload
}

func TestGetRemoteSmall(t *testing.T) {
	ctx := context.Background()

	cnr := container.New(container.WithPolicy(new(netmap.PlacementPolicy)))
	idCnr := container.CalculateID(cnr)

	newSvc := func(b *testPlacementBuilder, c *testClientCache) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = test.NewLogger(false)
		svc.localStorage = newTestStorage()
		svc.assembly = true

		const curEpoch = 13

		svc.traverserGenerator = &testTraverserGenerator{
			c: cnr,
			b: map[uint64]placement.Builder{
				curEpoch: b,
			},
		}
		svc.clientCache = c
		svc.currentEpochReceiver = testEpochReceiver(curEpoch)

		return svc
	}

	newPrm := func(raw bool, w ObjectWriter) Prm {
		p := Prm{}
		p.SetObjectWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		return p
	}

	newRngPrm := func(raw bool, w ChunkWriter, off, ln uint64) RangePrm {
		p := RangePrm{}
		p.SetChunkWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		r := objectSDK.NewRange()
		r.SetOffset(off)
		r.SetLength(ln)

		p.SetRange(r)

		return p
	}

	newHeadPrm := func(raw bool, w ObjectWriter) HeadPrm {
		p := HeadPrm{}
		p.SetHeaderWriter(w)
		p.WithRawFlag(raw)
		p.common = new(util.CommonPrm).WithLocalOnly(false)

		return p
	}

	t.Run("OK", func(t *testing.T) {
		addr := oidtest.Address()
		addr.SetContainer(idCnr)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.EncodeToString(): ns,
			},
		}

		payloadSz := uint64(10)
		payload := make([]byte, payloadSz)
		rand.Read(payload)

		obj := generateObject(addr, nil, payload)

		c1 := newTestClient()
		c1.addResult(addr, obj, nil)

		c2 := newTestClient()
		c2.addResult(addr, nil, errors.New("any error"))

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testClient{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		w := NewSimpleObjectWriter()

		p := newPrm(false, w)
		p.WithAddress(addr)

		err := svc.Get(ctx, p)
		require.NoError(t, err)
		require.Equal(t, obj, w.Object())

		*c1, *c2 = *c2, *c1

		err = svc.Get(ctx, p)
		require.NoError(t, err)
		require.Equal(t, obj, w.Object())

		w = NewSimpleObjectWriter()
		rngPrm := newRngPrm(false, w, payloadSz/3, payloadSz/3)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[payloadSz/3:2*payloadSz/3], w.Object().Payload())

		w = NewSimpleObjectWriter()
		headPrm := newHeadPrm(false, w)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), w.Object())
	})

	t.Run("INHUMED", func(t *testing.T) {
		addr := oidtest.Address()
		addr.SetContainer(idCnr)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.EncodeToString(): ns,
			},
		}

		c1 := newTestClient()
		c1.addResult(addr, nil, errors.New("any error"))

		c2 := newTestClient()
		c2.addResult(addr, nil, new(apistatus.ObjectAlreadyRemoved))

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testClient{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		p := newPrm(false, nil)
		p.WithAddress(addr)

		err := svc.Get(ctx, p)
		require.ErrorAs(t, err, new(*apistatus.ObjectAlreadyRemoved))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.ErrorAs(t, err, new(*apistatus.ObjectAlreadyRemoved))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.ErrorAs(t, err, new(*apistatus.ObjectAlreadyRemoved))
	})

	t.Run("404", func(t *testing.T) {
		addr := oidtest.Address()
		addr.SetContainer(idCnr)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.EncodeToString(): ns,
			},
		}

		c1 := newTestClient()
		c1.addResult(addr, nil, errors.New("any error"))

		c2 := newTestClient()
		c2.addResult(addr, nil, errors.New("any error"))

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testClient{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		p := newPrm(false, nil)
		p.WithAddress(addr)

		err := svc.Get(ctx, p)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("VIRTUAL", func(t *testing.T) {
		testHeadVirtual := func(svc *Service, addr oid.Address, i *objectSDK.SplitInfo) {
			headPrm := newHeadPrm(false, nil)
			headPrm.WithAddress(addr)

			errSplit := objectSDK.NewSplitInfoError(objectSDK.NewSplitInfo())

			err := svc.Head(ctx, headPrm)
			require.True(t, errors.As(err, &errSplit))
			require.Equal(t, i, errSplit.SplitInfo())
		}

		t.Run("linking", func(t *testing.T) {
			t.Run("get linking failure", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())

				var splitAddr oid.Address
				splitAddr.SetContainer(idCnr)
				idLink, _ := splitInfo.Link()
				splitAddr.SetObject(idLink)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.EncodeToString():      ns,
						splitAddr.EncodeToString(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				p := newPrm(false, nil)
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

				rngPrm := newRngPrm(false, nil, 0, 0)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				srcObj := generateObject(addr, nil, nil)
				srcObj.SetPayloadSize(10)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())

				children, childIDs, _ := generateChain(2, idCnr)

				var linkAddr oid.Address
				linkAddr.SetContainer(idCnr)
				idLink, _ := splitInfo.Link()
				linkAddr.SetObject(idLink)

				linkingObj := generateObject(linkAddr, nil, nil, childIDs...)
				linkingObj.SetParentID(addr.Object())
				linkingObj.SetParent(srcObj)

				var child1Addr oid.Address
				child1Addr.SetContainer(idCnr)
				child1Addr.SetObject(childIDs[0])

				var child2Addr oid.Address
				child2Addr.SetContainer(idCnr)
				child2Addr.SetObject(childIDs[1])

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(linkAddr, nil, errors.New("any error"))
				c1.addResult(child1Addr, nil, errors.New("any error"))
				c1.addResult(child2Addr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(linkAddr, linkingObj, nil)
				c2.addResult(child1Addr, children[0], nil)
				c2.addResult(child2Addr, nil, apistatus.ObjectNotFound{})

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.EncodeToString():       ns,
						linkAddr.EncodeToString():   ns,
						child1Addr.EncodeToString(): ns,
						child2Addr.EncodeToString(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				p := newPrm(false, NewSimpleObjectWriter())
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

				rngPrm := newRngPrm(false, NewSimpleObjectWriter(), 0, 1)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			})

			t.Run("OK", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				srcObj := generateObject(addr, nil, nil)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())

				children, childIDs, payload := generateChain(2, idCnr)
				srcObj.SetPayload(payload)
				srcObj.SetPayloadSize(uint64(len(payload)))
				children[len(children)-1].SetParent(srcObj)

				var linkAddr oid.Address
				linkAddr.SetContainer(idCnr)
				idLink, _ := splitInfo.Link()
				linkAddr.SetObject(idLink)

				linkingObj := generateObject(linkAddr, nil, nil, childIDs...)
				linkingObj.SetParentID(addr.Object())
				linkingObj.SetParent(srcObj)

				var child1Addr oid.Address
				child1Addr.SetContainer(idCnr)
				child1Addr.SetObject(childIDs[0])

				var child2Addr oid.Address
				child2Addr.SetContainer(idCnr)
				child2Addr.SetObject(childIDs[1])

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(linkAddr, nil, errors.New("any error"))
				c1.addResult(child1Addr, nil, errors.New("any error"))
				c1.addResult(child2Addr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(linkAddr, linkingObj, nil)
				c2.addResult(child1Addr, children[0], nil)
				c2.addResult(child2Addr, children[1], nil)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.EncodeToString():       ns,
						linkAddr.EncodeToString():   ns,
						child1Addr.EncodeToString(): ns,
						child2Addr.EncodeToString(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				w := NewSimpleObjectWriter()

				p := newPrm(false, w)
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.NoError(t, err)
				require.Equal(t, srcObj, w.Object())

				w = NewSimpleObjectWriter()
				payloadSz := srcObj.PayloadSize()

				off := payloadSz / 3
				ln := payloadSz / 3

				rngPrm := newRngPrm(false, w, off, ln)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.NoError(t, err)
				require.Equal(t, payload[off:off+ln], w.Object().Payload())
			})
		})

		t.Run("right child", func(t *testing.T) {
			t.Run("get right child failure", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())

				var splitAddr oid.Address
				splitAddr.SetContainer(idCnr)
				idLast, _ := splitInfo.LastPart()
				splitAddr.SetObject(idLast)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.EncodeToString():      ns,
						splitAddr.EncodeToString(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				p := newPrm(false, nil)
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

				rngPrm := newRngPrm(false, nil, 0, 0)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				srcObj := generateObject(addr, nil, nil)
				srcObj.SetPayloadSize(10)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())

				children, _, _ := generateChain(2, idCnr)

				var rightAddr oid.Address
				rightAddr.SetContainer(idCnr)
				idLast, _ := splitInfo.LastPart()
				rightAddr.SetObject(idLast)

				rightObj := children[len(children)-1]

				rightObj.SetParentID(addr.Object())
				rightObj.SetParent(srcObj)

				preRightAddr := object.AddressOf(children[len(children)-2])

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(rightAddr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(rightAddr, rightObj, nil)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.EncodeToString():         ns,
						rightAddr.EncodeToString():    ns,
						preRightAddr.EncodeToString(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				headSvc := newTestClient()
				headSvc.addResult(preRightAddr, nil, apistatus.ObjectNotFound{})

				p := newPrm(false, NewSimpleObjectWriter())
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

				rngPrm := newRngPrm(false, nil, 0, 1)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			})

			t.Run("OK", func(t *testing.T) {
				addr := oidtest.Address()
				addr.SetContainer(idCnr)
				addr.SetObject(oidtest.ID())

				srcObj := generateObject(addr, nil, nil)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())

				children, _, payload := generateChain(2, idCnr)
				srcObj.SetPayloadSize(uint64(len(payload)))
				srcObj.SetPayload(payload)

				rightObj := children[len(children)-1]

				idLast, _ := splitInfo.LastPart()
				rightObj.SetID(idLast)
				rightObj.SetParentID(addr.Object())
				rightObj.SetParent(srcObj)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))

				for i := range children {
					c1.addResult(object.AddressOf(children[i]), nil, errors.New("any error"))
				}

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))

				for i := range children {
					c2.addResult(object.AddressOf(children[i]), children[i], nil)
				}

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{},
				}

				builder.vectors[addr.EncodeToString()] = ns

				for i := range children {
					builder.vectors[object.AddressOf(children[i]).EncodeToString()] = ns
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				testHeadVirtual(svc, addr, splitInfo)

				w := NewSimpleObjectWriter()

				p := newPrm(false, w)
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.NoError(t, err)
				require.Equal(t, srcObj, w.Object())

				w = NewSimpleObjectWriter()
				payloadSz := srcObj.PayloadSize()

				off := payloadSz / 3
				ln := payloadSz / 3

				rngPrm := newRngPrm(false, w, off, ln)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.NoError(t, err)
				require.Equal(t, payload[off:off+ln], w.Object().Payload())

				w = NewSimpleObjectWriter()
				off = payloadSz - 2
				ln = 1

				rngPrm = newRngPrm(false, w, off, ln)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.NoError(t, err)
				require.Equal(t, payload[off:off+ln], w.Object().Payload())
			})
		})
	})
}

func TestGetFromPastEpoch(t *testing.T) {
	ctx := context.Background()

	cnr := container.New(container.WithPolicy(new(netmap.PlacementPolicy)))
	idCnr := container.CalculateID(cnr)

	addr := oidtest.Address()
	addr.SetContainer(idCnr)

	payloadSz := uint64(10)
	payload := make([]byte, payloadSz)
	_, _ = rand.Read(payload)

	obj := generateObject(addr, nil, payload)

	ns, as := testNodeMatrix(t, []int{2, 2})

	c11 := newTestClient()
	c11.addResult(addr, nil, errors.New("any error"))

	c12 := newTestClient()
	c12.addResult(addr, nil, errors.New("any error"))

	c21 := newTestClient()
	c21.addResult(addr, nil, errors.New("any error"))

	c22 := newTestClient()
	c22.addResult(addr, obj, nil)

	svc := &Service{cfg: new(cfg)}
	svc.log = test.NewLogger(false)
	svc.localStorage = newTestStorage()
	svc.assembly = true

	const curEpoch = 13

	svc.traverserGenerator = &testTraverserGenerator{
		c: cnr,
		b: map[uint64]placement.Builder{
			curEpoch: &testPlacementBuilder{
				vectors: map[string][]netmap.Nodes{
					addr.EncodeToString(): ns[:1],
				},
			},
			curEpoch - 1: &testPlacementBuilder{
				vectors: map[string][]netmap.Nodes{
					addr.EncodeToString(): ns[1:],
				},
			},
		},
	}

	svc.clientCache = &testClientCache{
		clients: map[string]*testClient{
			as[0][0]: c11,
			as[0][1]: c12,
			as[1][0]: c21,
			as[1][1]: c22,
		},
	}

	svc.currentEpochReceiver = testEpochReceiver(curEpoch)

	w := NewSimpleObjectWriter()

	commonPrm := new(util.CommonPrm)

	p := Prm{}
	p.SetObjectWriter(w)
	p.SetCommonParameters(commonPrm)
	p.WithAddress(addr)

	err := svc.Get(ctx, p)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	commonPrm.SetNetmapLookupDepth(1)

	err = svc.Get(ctx, p)
	require.NoError(t, err)
	require.Equal(t, obj, w.Object())

	rp := RangePrm{}
	rp.SetChunkWriter(w)
	commonPrm.SetNetmapLookupDepth(0)
	rp.SetCommonParameters(commonPrm)
	rp.WithAddress(addr)

	off, ln := payloadSz/3, payloadSz/3

	r := objectSDK.NewRange()
	r.SetOffset(off)
	r.SetLength(ln)

	rp.SetRange(r)

	err = svc.GetRange(ctx, rp)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	w = NewSimpleObjectWriter()
	rp.SetChunkWriter(w)
	commonPrm.SetNetmapLookupDepth(1)

	err = svc.GetRange(ctx, rp)
	require.NoError(t, err)
	require.Equal(t, payload[off:off+ln], w.Object().Payload())

	hp := HeadPrm{}
	hp.SetHeaderWriter(w)
	commonPrm.SetNetmapLookupDepth(0)
	hp.SetCommonParameters(commonPrm)
	hp.WithAddress(addr)

	err = svc.Head(ctx, hp)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	w = NewSimpleObjectWriter()
	hp.SetHeaderWriter(w)
	commonPrm.SetNetmapLookupDepth(1)

	err = svc.Head(ctx, hp)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload(), w.Object())
}
