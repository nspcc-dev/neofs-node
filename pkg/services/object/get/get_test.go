package getsvc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	inhumed map[string]struct{}

	virtual map[string]*objectSDK.SplitInfo

	phy map[string]*object.Object
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
		obj *object.RawObject
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
		phy:     make(map[string]*object.Object),
	}
}

func (g *testTraverserGenerator) GenerateTraverser(addr *objectSDK.Address, e uint64) (*placement.Traverser, error) {
	return placement.NewTraverser(
		placement.ForContainer(g.c),
		placement.ForObject(addr.ObjectID()),
		placement.UseBuilder(g.b[e]),
		placement.SuccessAfter(1),
	)
}

func (p *testPlacementBuilder) BuildPlacement(addr *objectSDK.Address, _ *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	vs, ok := p.vectors[addr.String()]
	if !ok {
		return nil, errors.New("vectors for address not found")
	}

	return vs, nil
}

func (c *testClientCache) get(addr string) (getClient, error) {
	v, ok := c.clients[addr]
	if !ok {
		return nil, errors.New("could not construct client")
	}

	return v, nil
}

func newTestClient() *testClient {
	return &testClient{
		results: map[string]struct {
			obj *object.RawObject
			err error
		}{},
	}
}

func (c *testClient) getObject(exec *execCtx) (*objectSDK.Object, error) {
	v, ok := c.results[exec.address().String()]
	if !ok {
		return nil, object.ErrNotFound
	}

	if v.err != nil {
		return nil, v.err
	}

	return cutToRange(v.obj.Object(), exec.ctxRange()).SDK(), nil
}

func (c *testClient) addResult(addr *objectSDK.Address, obj *object.RawObject, err error) {
	c.results[addr.String()] = struct {
		obj *object.RawObject
		err error
	}{obj: obj, err: err}
}

func (s *testStorage) get(exec *execCtx) (*object.Object, error) {
	var (
		ok    bool
		obj   *object.Object
		sAddr = exec.address().String()
	)

	if _, ok = s.inhumed[sAddr]; ok {
		return nil, object.ErrAlreadyRemoved
	}

	if info, ok := s.virtual[sAddr]; ok {
		return nil, objectSDK.NewSplitInfoError(info)
	}

	if obj, ok = s.phy[sAddr]; ok {
		return cutToRange(obj, exec.ctxRange()), nil
	}

	return nil, object.ErrNotFound
}

func cutToRange(o *object.Object, rng *objectSDK.Range) *object.Object {
	obj := object.NewRawFromObject(o)

	if rng == nil {
		return obj.Object()
	}

	from := rng.GetOffset()
	to := from + rng.GetLength()

	payload := obj.Payload()

	obj = obj.CutPayload()
	obj.SetPayload(payload[from:to])

	return obj.Object()
}

func (s *testStorage) addPhy(addr *objectSDK.Address, obj *object.RawObject) {
	s.phy[addr.String()] = obj.Object()
}

func (s *testStorage) addVirtual(addr *objectSDK.Address, info *objectSDK.SplitInfo) {
	s.virtual[addr.String()] = info
}

func (s *testStorage) inhume(addr *objectSDK.Address) {
	s.inhumed[addr.String()] = struct{}{}
}

func testSHA256() (cs [sha256.Size]byte) {
	rand.Read(cs[:])
	return cs
}

func generateID() *objectSDK.ID {
	id := objectSDK.NewID()
	id.SetSHA256(testSHA256())

	return id
}

func generateAddress() *objectSDK.Address {
	addr := objectSDK.NewAddress()
	addr.SetObjectID(generateID())

	cid := container.NewID()
	cid.SetSHA256(testSHA256())

	addr.SetContainerID(cid)

	return addr
}

func generateObject(addr *objectSDK.Address, prev *objectSDK.ID, payload []byte, children ...*objectSDK.ID) *object.RawObject {
	obj := object.NewRaw()
	obj.SetContainerID(addr.ContainerID())
	obj.SetID(addr.ObjectID())
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))
	obj.SetPreviousID(prev)
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

		addr := generateAddress()
		obj := generateObject(addr, nil, payload)

		storage.addPhy(addr, obj)

		p.WithAddress(addr)

		storage.addPhy(addr, obj)

		err := svc.Get(ctx, p)

		require.NoError(t, err)

		require.Equal(t, obj.Object(), w.Object())

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
		require.Equal(t, obj.CutPayload().Object(), w.Object())
	})

	t.Run("INHUMED", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := generateAddress()

		storage.inhume(addr)

		p.WithAddress(addr)

		err := svc.Get(ctx, p)

		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))
	})

	t.Run("404", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := generateAddress()

		p.WithAddress(addr)

		err := svc.Get(ctx, p)

		require.True(t, errors.Is(err, object.ErrNotFound))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)

		require.True(t, errors.Is(err, object.ErrNotFound))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.True(t, errors.Is(err, object.ErrNotFound))
	})

	t.Run("VIRTUAL", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(true, nil)

		addr := generateAddress()

		splitInfo := objectSDK.NewSplitInfo()
		splitInfo.SetSplitID(objectSDK.NewSplitID())
		splitInfo.SetLink(generateID())
		splitInfo.SetLastPart(generateID())

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

			var err error
			na, err := network.AddressFromString(a)
			require.NoError(t, err)

			as[j], err = na.HostAddrString()
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

func generateChain(ln int, cid *container.ID) ([]*object.RawObject, []*objectSDK.ID, []byte) {
	curID := generateID()
	var prevID *objectSDK.ID

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)

	res := make([]*object.RawObject, 0, ln)
	ids := make([]*objectSDK.ID, 0, ln)
	payload := make([]byte, 0, ln*10)

	for i := 0; i < ln; i++ {
		ids = append(ids, curID)
		addr.SetObjectID(curID)

		payloadPart := make([]byte, 10)
		rand.Read(payloadPart)

		o := generateObject(addr, prevID, []byte{byte(i)})
		o.SetPayload(payloadPart)
		o.SetPayloadSize(uint64(len(payloadPart)))
		o.SetID(curID)

		payload = append(payload, payloadPart...)

		res = append(res, o)

		prevID = curID
		curID = generateID()
	}

	return res, ids, payload
}

func TestGetRemoteSmall(t *testing.T) {
	ctx := context.Background()

	cnr := container.New(container.WithPolicy(new(netmap.PlacementPolicy)))
	cid := container.CalculateID(cnr)

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
		addr := generateAddress()
		addr.SetContainerID(cid)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.String(): ns,
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
		require.Equal(t, obj.Object(), w.Object())

		*c1, *c2 = *c2, *c1

		err = svc.Get(ctx, p)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), w.Object())

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
		require.Equal(t, obj.CutPayload().Object(), w.Object())
	})

	t.Run("INHUMED", func(t *testing.T) {
		addr := generateAddress()
		addr.SetContainerID(cid)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.String(): ns,
			},
		}

		c1 := newTestClient()
		c1.addResult(addr, nil, errors.New("any error"))

		c2 := newTestClient()
		c2.addResult(addr, nil, object.ErrAlreadyRemoved)

		svc := newSvc(builder, &testClientCache{
			clients: map[string]*testClient{
				as[0][0]: c1,
				as[0][1]: c2,
			},
		})

		p := newPrm(false, nil)
		p.WithAddress(addr)

		err := svc.Get(ctx, p)
		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))
	})

	t.Run("404", func(t *testing.T) {
		addr := generateAddress()
		addr.SetContainerID(cid)

		ns, as := testNodeMatrix(t, []int{2})

		builder := &testPlacementBuilder{
			vectors: map[string][]netmap.Nodes{
				addr.String(): ns,
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
		require.True(t, errors.Is(err, object.ErrNotFound))

		rngPrm := newRngPrm(false, nil, 0, 0)
		rngPrm.WithAddress(addr)

		err = svc.GetRange(ctx, rngPrm)
		require.True(t, errors.Is(err, object.ErrNotFound))

		headPrm := newHeadPrm(false, nil)
		headPrm.WithAddress(addr)

		err = svc.Head(ctx, headPrm)
		require.True(t, errors.Is(err, object.ErrNotFound))
	})

	t.Run("VIRTUAL", func(t *testing.T) {
		testHeadVirtual := func(svc *Service, addr *objectSDK.Address, i *objectSDK.SplitInfo) {
			headPrm := newHeadPrm(false, nil)
			headPrm.WithAddress(addr)

			errSplit := objectSDK.NewSplitInfoError(objectSDK.NewSplitInfo())

			err := svc.Head(ctx, headPrm)
			require.True(t, errors.As(err, &errSplit))
			require.Equal(t, i, errSplit.SplitInfo())
		}

		t.Run("linking", func(t *testing.T) {
			t.Run("get linking failure", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(generateID())

				splitAddr := objectSDK.NewAddress()
				splitAddr.SetContainerID(cid)
				splitAddr.SetObjectID(splitInfo.Link())

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, object.ErrNotFound)

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, object.ErrNotFound)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.String():      ns,
						splitAddr.String(): ns,
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
				require.True(t, errors.Is(err, object.ErrNotFound))

				rngPrm := newRngPrm(false, nil, 0, 0)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)
				srcObj.SetPayloadSize(10)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(generateID())

				children, childIDs, _ := generateChain(2, cid)

				linkAddr := objectSDK.NewAddress()
				linkAddr.SetContainerID(cid)
				linkAddr.SetObjectID(splitInfo.Link())

				linkingObj := generateObject(linkAddr, nil, nil, childIDs...)
				linkingObj.SetParentID(addr.ObjectID())
				linkingObj.SetParent(srcObj.Object().SDK())

				child1Addr := objectSDK.NewAddress()
				child1Addr.SetContainerID(cid)
				child1Addr.SetObjectID(childIDs[0])

				child2Addr := objectSDK.NewAddress()
				child2Addr.SetContainerID(cid)
				child2Addr.SetObjectID(childIDs[1])

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(linkAddr, nil, errors.New("any error"))
				c1.addResult(child1Addr, nil, errors.New("any error"))
				c1.addResult(child2Addr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(linkAddr, linkingObj, nil)
				c2.addResult(child1Addr, children[0], nil)
				c2.addResult(child2Addr, nil, object.ErrNotFound)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.String():       ns,
						linkAddr.String():   ns,
						child1Addr.String(): ns,
						child2Addr.String(): ns,
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
				require.True(t, errors.Is(err, object.ErrNotFound))

				rngPrm := newRngPrm(false, NewSimpleObjectWriter(), 0, 1)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("OK", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLink(generateID())

				children, childIDs, payload := generateChain(2, cid)
				srcObj.SetPayload(payload)
				srcObj.SetPayloadSize(uint64(len(payload)))
				children[len(children)-1].SetParent(srcObj.Object().SDK())

				linkAddr := objectSDK.NewAddress()
				linkAddr.SetContainerID(cid)
				linkAddr.SetObjectID(splitInfo.Link())

				linkingObj := generateObject(linkAddr, nil, nil, childIDs...)
				linkingObj.SetParentID(addr.ObjectID())
				linkingObj.SetParent(srcObj.Object().SDK())

				child1Addr := objectSDK.NewAddress()
				child1Addr.SetContainerID(cid)
				child1Addr.SetObjectID(childIDs[0])

				child2Addr := objectSDK.NewAddress()
				child2Addr.SetContainerID(cid)
				child2Addr.SetObjectID(childIDs[1])

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
						addr.String():       ns,
						linkAddr.String():   ns,
						child1Addr.String(): ns,
						child2Addr.String(): ns,
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
				require.Equal(t, srcObj.Object(), w.Object())

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
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(generateID())

				splitAddr := objectSDK.NewAddress()
				splitAddr.SetContainerID(cid)
				splitAddr.SetObjectID(splitInfo.LastPart())

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, object.ErrNotFound)

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, object.ErrNotFound)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.String():      ns,
						splitAddr.String(): ns,
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
				require.True(t, errors.Is(err, object.ErrNotFound))

				rngPrm := newRngPrm(false, nil, 0, 0)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)
				srcObj.SetPayloadSize(10)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(generateID())

				children, _, _ := generateChain(2, cid)

				rightAddr := objectSDK.NewAddress()
				rightAddr.SetContainerID(cid)
				rightAddr.SetObjectID(splitInfo.LastPart())

				rightObj := children[len(children)-1]

				rightObj.SetParentID(addr.ObjectID())
				rightObj.SetParent(srcObj.Object().SDK())

				preRightAddr := children[len(children)-2].Object().Address()

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(rightAddr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))
				c2.addResult(rightAddr, rightObj, nil)

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{
						addr.String():         ns,
						rightAddr.String():    ns,
						preRightAddr.String(): ns,
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
				headSvc.addResult(preRightAddr, nil, object.ErrNotFound)

				p := newPrm(false, NewSimpleObjectWriter())
				p.WithAddress(addr)

				err := svc.Get(ctx, p)
				require.True(t, errors.Is(err, object.ErrNotFound))

				rngPrm := newRngPrm(false, nil, 0, 1)
				rngPrm.WithAddress(addr)

				err = svc.GetRange(ctx, rngPrm)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("OK", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := objectSDK.NewSplitInfo()
				splitInfo.SetLastPart(generateID())

				children, _, payload := generateChain(2, cid)
				srcObj.SetPayloadSize(uint64(len(payload)))
				srcObj.SetPayload(payload)

				rightObj := children[len(children)-1]

				rightObj.SetID(splitInfo.LastPart())
				rightObj.SetParentID(addr.ObjectID())
				rightObj.SetParent(srcObj.Object().SDK())

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))

				for i := range children {
					c1.addResult(children[i].Object().Address(), nil, errors.New("any error"))
				}

				c2 := newTestClient()
				c2.addResult(addr, nil, objectSDK.NewSplitInfoError(splitInfo))

				for i := range children {
					c2.addResult(children[i].Object().Address(), children[i], nil)
				}

				builder := &testPlacementBuilder{
					vectors: map[string][]netmap.Nodes{},
				}

				builder.vectors[addr.String()] = ns

				for i := range children {
					builder.vectors[children[i].Object().Address().String()] = ns
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
				require.Equal(t, srcObj.Object(), w.Object())

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
	cid := container.CalculateID(cnr)

	addr := generateAddress()
	addr.SetContainerID(cid)

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
	require.True(t, errors.Is(err, object.ErrNotFound))

	commonPrm.SetNetmapLookupDepth(1)

	err = svc.Get(ctx, p)
	require.NoError(t, err)
	require.Equal(t, obj.Object(), w.Object())

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
	require.True(t, errors.Is(err, object.ErrNotFound))

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
	require.True(t, errors.Is(err, object.ErrNotFound))

	w = NewSimpleObjectWriter()
	hp.SetHeaderWriter(w)
	commonPrm.SetNetmapLookupDepth(1)

	err = svc.Head(ctx, hp)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload().Object(), w.Object())
}
