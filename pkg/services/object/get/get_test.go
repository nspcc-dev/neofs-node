package getsvc

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testStorage struct {
	unimplementedLocalStorage

	inhumed map[oid.Address]struct{}

	virtual map[oid.Address]*object.SplitInfo

	phy map[oid.Address]*object.Object
}

type testNeoFS struct {
	vectors map[oid.Address][][]netmap.NodeInfo
}

type testClientCache struct {
	clients map[string]*testClient
}

type testClient struct {
	results map[oid.Address]struct {
		obj *object.Object
		err error
	}
}

func newTestStorage() *testStorage {
	return &testStorage{
		inhumed: make(map[oid.Address]struct{}),
		virtual: make(map[oid.Address]*object.SplitInfo),
		phy:     make(map[oid.Address]*object.Object),
	}
}

func (g *testNeoFS) IsLocalNodePublicKey([]byte) bool { return false }

func (g *testNeoFS) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	nodeLists, ok := g.vectors[addr]
	if !ok {
		return nil, nil, nil, errors.New("vectors for address not found")
	}

	primaryNums := make([]uint, len(nodeLists))
	for i := range primaryNums {
		primaryNums[i] = 1
	}

	return nodeLists, primaryNums, nil, nil
}

func (c *testClientCache) get(_ context.Context, info client.NodeInfo) (getClient, error) {
	v, ok := c.clients[info.AddressGroup().String()]
	if !ok {
		return nil, errors.New("could not construct client")
	}

	return v, nil
}

func newTestClient() *testClient {
	return &testClient{
		results: map[oid.Address]struct {
			obj *object.Object
			err error
		}{},
	}
}

func (c *testClient) getObject(exec *execCtx) (*object.Object, io.ReadCloser, error) {
	v, ok := c.results[exec.address()]
	if !ok {
		var errNotFound apistatus.ObjectNotFound

		return nil, nil, errNotFound
	}

	if v.err != nil {
		return nil, nil, v.err
	}

	obj := cutToRange(v.obj, exec.ctxRange())

	if obj != nil && len(obj.Payload()) > 0 {
		reader := io.NopCloser(bytes.NewReader(obj.Payload()))
		objWithoutPayload := obj.CutPayload()
		objWithoutPayload.SetPayloadSize(obj.PayloadSize())
		return objWithoutPayload, reader, nil
	}

	return obj, nil, nil
}

func (c *testClient) addResult(addr oid.Address, obj *object.Object, err error) {
	c.results[addr] = struct {
		obj *object.Object
		err error
	}{obj: obj, err: err}
}

func (s *testStorage) get(exec *execCtx) (*object.Object, io.ReadCloser, error) {
	var (
		ok    bool
		obj   *object.Object
		sAddr = exec.address()
	)

	if _, ok = s.inhumed[sAddr]; ok {
		var errRemoved apistatus.ObjectAlreadyRemoved

		return nil, nil, errRemoved
	}

	if info, ok := s.virtual[sAddr]; ok {
		return nil, nil, object.NewSplitInfoError(info)
	}

	if obj, ok = s.phy[sAddr]; ok {
		obj = cutToRange(obj, exec.ctxRange())

		if obj != nil && len(obj.Payload()) > 0 {
			reader := io.NopCloser(bytes.NewReader(obj.Payload()))
			objWithoutPayload := obj.CutPayload()
			objWithoutPayload.SetPayloadSize(obj.PayloadSize())
			return objWithoutPayload, reader, nil
		}

		return obj, nil, nil
	}

	var errNotFound apistatus.ObjectNotFound

	return nil, nil, errNotFound
}

func (s *testStorage) Head(addr oid.Address, _ bool) (*object.Object, error) {
	hdr, _, err := s.get(&execCtx{
		prm: RangePrm{
			commonPrm: commonPrm{
				addr: addr,
			},
		},
	})
	return hdr, err
}

func cutToRange(o *object.Object, rng *object.Range) *object.Object {
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

func (s *testStorage) addPhy(addr oid.Address, obj *object.Object) {
	s.phy[addr] = obj
}

func (s *testStorage) addVirtual(addr oid.Address, info *object.SplitInfo) {
	s.virtual[addr] = info
}

func (s *testStorage) inhume(addr oid.Address) {
	s.inhumed[addr] = struct{}{}
}

func generateObject(addr oid.Address, prev *oid.ID, payload []byte, children ...oid.ID) *object.Object {
	obj := new(object.Object)
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
	addr := oidtest.Address()
	anyNodeLists, _ := testNodeMatrix(t, []int{2})

	newSvc := func(storage *testStorage) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = zaptest.NewLogger(t)
		svc.localObjects = storage
		svc.localStorage = storage
		svc.neoFSNet = &testNeoFS{
			vectors: map[oid.Address][][]netmap.NodeInfo{
				addr: anyNodeLists,
			},
		}

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

		r := object.NewRange()
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
		_, _ = rand.Read(payload)

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

		testSplit := func(addr oid.Address, si *object.SplitInfo) {
			p.WithAddress(addr)

			storage.addVirtual(addr, si)

			err := svc.Get(ctx, p)

			errSplit := object.NewSplitInfoError(object.NewSplitInfo())

			require.True(t, errors.As(err, &errSplit))

			require.Equal(t, si, errSplit.SplitInfo())

			rngPrm := newRngPrm(true, nil, 0, 0)
			rngPrm.WithAddress(addr)

			err = svc.Get(ctx, p)

			require.True(t, errors.As(err, &errSplit))

			headPrm := newHeadPrm(true, nil)
			headPrm.WithAddress(addr)

			err = svc.Head(ctx, headPrm)
			require.True(t, errors.As(err, &errSplit))
			require.Equal(t, si, errSplit.SplitInfo())
		}

		t.Run("V1 split", func(t *testing.T) {
			splitInfo := object.NewSplitInfo()
			splitInfo.SetSplitID(object.NewSplitID())
			splitInfo.SetLink(oidtest.ID())
			splitInfo.SetLastPart(oidtest.ID())

			testSplit(addr, splitInfo)
		})

		t.Run("V2 split", func(t *testing.T) {
			splitInfo := object.NewSplitInfo()
			splitInfo.SetLink(oidtest.ID())
			splitInfo.SetLastPart(oidtest.ID())
			splitInfo.SetFirstPart(oidtest.ID())

			testSplit(addr, splitInfo)
		})
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

			as[j] = na.String()

			ns[j] = ni
		}

		mNodes[i] = ns
		mAddr[i] = as
	}

	return mNodes, mAddr
}

func generateChain(ln int, cnr cid.ID) ([]*object.Object, []oid.ID, []byte) {
	curID := oidtest.ID()
	var prevID *oid.ID

	var addr oid.Address
	addr.SetContainer(cnr)

	res := make([]*object.Object, 0, ln)
	ids := make([]oid.ID, 0, ln)
	payload := make([]byte, 0, ln*10)

	for i := range ln {
		ids = append(ids, curID)
		addr.SetObject(curID)

		payloadPart := make([]byte, 10)
		_, _ = rand.Read(payloadPart)

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

	idCnr := cidtest.ID()

	newSvc := func(vectors map[oid.Address][][]netmap.NodeInfo, c *testClientCache) *Service {
		svc := &Service{cfg: new(cfg)}
		svc.log = zaptest.NewLogger(t)
		svc.localStorage = newTestStorage()

		svc.neoFSNet = &testNeoFS{
			vectors: vectors,
		}
		svc.clientCache = c

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

		r := object.NewRange()
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

		vectors := map[oid.Address][][]netmap.NodeInfo{
			addr: ns,
		}

		payloadSz := uint64(10)
		payload := make([]byte, payloadSz)
		_, _ = rand.Read(payload)

		obj := generateObject(addr, nil, payload)

		c1 := newTestClient()
		c1.addResult(addr, obj, nil)

		c2 := newTestClient()
		c2.addResult(addr, nil, errors.New("any error"))

		svc := newSvc(vectors, &testClientCache{
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

		vectors := map[oid.Address][][]netmap.NodeInfo{
			addr: ns,
		}

		c1 := newTestClient()
		c1.addResult(addr, nil, errors.New("any error"))

		c2 := newTestClient()
		c2.addResult(addr, nil, new(apistatus.ObjectAlreadyRemoved))

		svc := newSvc(vectors, &testClientCache{
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

		vectors := map[oid.Address][][]netmap.NodeInfo{
			addr: ns,
		}

		c1 := newTestClient()
		c1.addResult(addr, nil, errors.New("any error"))

		c2 := newTestClient()
		c2.addResult(addr, nil, errors.New("any error"))

		svc := newSvc(vectors, &testClientCache{
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
		testHeadVirtual := func(svc *Service, addr oid.Address, i *object.SplitInfo) {
			headPrm := newHeadPrm(false, nil)
			headPrm.WithAddress(addr)

			errSplit := object.NewSplitInfoError(object.NewSplitInfo())

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

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				var splitAddr oid.Address
				splitAddr.SetContainer(idCnr)
				idLink := splitInfo.GetLink()
				splitAddr.SetObject(idLink)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				c2 := newTestClient()
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				vectors := map[oid.Address][][]netmap.NodeInfo{
					addr:      ns,
					splitAddr: ns,
				}

				svc := newSvc(vectors, &testClientCache{
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

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				children, childIDs, _ := generateChain(2, idCnr)

				var linkAddr oid.Address
				linkAddr.SetContainer(idCnr)
				idLink := splitInfo.GetLink()
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
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))
				c2.addResult(linkAddr, linkingObj, nil)
				c2.addResult(child1Addr, children[0], nil)
				c2.addResult(child2Addr, nil, apistatus.ObjectNotFound{})

				vectors := map[oid.Address][][]netmap.NodeInfo{
					addr:       ns,
					linkAddr:   ns,
					child1Addr: ns,
					child2Addr: ns,
				}

				svc := newSvc(vectors, &testClientCache{
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

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLink(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				children, childIDs, payload := generateChain(2, idCnr)
				srcObj.SetPayload(payload)
				srcObj.SetPayloadSize(uint64(len(payload)))
				children[len(children)-1].SetParent(srcObj)

				var linkAddr oid.Address
				linkAddr.SetContainer(idCnr)
				idLink := splitInfo.GetLink()
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
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))
				c2.addResult(linkAddr, linkingObj, nil)
				c2.addResult(child1Addr, children[0], nil)
				c2.addResult(child2Addr, children[1], nil)

				vectors := map[oid.Address][][]netmap.NodeInfo{
					addr:       ns,
					linkAddr:   ns,
					child1Addr: ns,
					child2Addr: ns,
				}

				svc := newSvc(vectors, &testClientCache{
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

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				var splitAddr oid.Address
				splitAddr.SetContainer(idCnr)
				idLast := splitInfo.GetLastPart()
				splitAddr.SetObject(idLast)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				c2 := newTestClient()
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))
				c2.addResult(splitAddr, nil, apistatus.ObjectNotFound{})

				vectors := map[oid.Address][][]netmap.NodeInfo{
					addr:      ns,
					splitAddr: ns,
				}

				svc := newSvc(vectors, &testClientCache{
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
				srcObj.SetPayloadSize(11)

				ns, as := testNodeMatrix(t, []int{2})

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				children, _, _ := generateChain(2, idCnr)

				var rightAddr oid.Address
				rightAddr.SetContainer(idCnr)
				idLast := splitInfo.GetLastPart()
				rightAddr.SetObject(idLast)

				rightObj := children[len(children)-1]

				rightObj.SetParentID(addr.Object())
				rightObj.SetParent(srcObj)

				preRightAddr := children[len(children)-2].Address()

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))
				c1.addResult(rightAddr, nil, errors.New("any error"))

				c2 := newTestClient()
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))
				c2.addResult(rightAddr, rightObj, nil)

				vectors := map[oid.Address][][]netmap.NodeInfo{
					addr:         ns,
					rightAddr:    ns,
					preRightAddr: ns,
				}

				svc := newSvc(vectors, &testClientCache{
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

				splitInfo := object.NewSplitInfo()
				splitInfo.SetLastPart(oidtest.ID())
				splitInfo.SetSplitID(object.NewSplitID())

				children, _, payload := generateChain(2, idCnr)
				srcObj.SetPayloadSize(uint64(len(payload)))
				srcObj.SetPayload(payload)

				rightObj := children[len(children)-1]

				idLast := splitInfo.GetLastPart()
				rightObj.SetID(idLast)
				rightObj.SetParentID(addr.Object())
				rightObj.SetParent(srcObj)

				c1 := newTestClient()
				c1.addResult(addr, nil, errors.New("any error"))

				for i := range children {
					c1.addResult(children[i].Address(), nil, errors.New("any error"))
				}

				c2 := newTestClient()
				c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))

				for i := range children {
					c2.addResult(children[i].Address(), children[i], nil)
				}

				vectors := map[oid.Address][][]netmap.NodeInfo{}

				vectors[addr] = ns

				for i := range children {
					vectors[children[i].Address()] = ns
				}

				svc := newSvc(vectors, &testClientCache{
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

func parameterizeXHeaders(t testing.TB, p *Prm, ss []string) {
	xs := make([]*protosession.XHeader, len(ss))
	for i := 0; i < len(ss); i += 2 {
		xs[i] = &protosession.XHeader{Key: ss[i], Value: ss[i+1]}
	}

	cp, err := util.CommonPrmFromRequest(&protoobject.GetRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			XHeaders: xs,
		},
	})
	require.NoError(t, err)

	p.SetCommonParameters(cp)
}

type failingReader struct {
	data      []byte
	pos       int
	failAfter int
	err       error
}

func (r *failingReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	if r.pos >= r.failAfter && r.failAfter > 0 {
		return 0, r.err
	}

	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *failingReader) Close() error {
	return nil
}

type trackingWriter struct {
	writeHeaderCount atomic.Int32
	writeChunkCount  atomic.Int32
	failAfterChunks  int32
	err              error
}

func (w *trackingWriter) WriteHeader(*object.Object) error {
	w.writeHeaderCount.Add(1)
	return nil
}

func (w *trackingWriter) WriteChunk([]byte) error {
	count := w.writeChunkCount.Add(1)

	if w.failAfterChunks > 0 && count == w.failAfterChunks {
		return w.err
	}
	return nil
}

type testStorageWithFailingReader struct {
	unimplementedLocalStorage
	obj       *object.Object
	failAfter int
	err       error
}

func (s *testStorageWithFailingReader) get(*execCtx) (*object.Object, io.ReadCloser, error) {
	if s.obj == nil {
		return nil, nil, errors.New("object not found")
	}

	payload := s.obj.Payload()
	reader := &failingReader{
		data:      payload,
		failAfter: s.failAfter,
		err:       s.err,
	}

	objWithoutPayload := s.obj.CutPayload()
	objWithoutPayload.SetPayloadSize(s.obj.PayloadSize())
	return objWithoutPayload, reader, nil
}

func (s *testStorageWithFailingReader) Head(oid.Address, bool) (*object.Object, error) {
	if s.obj == nil {
		return nil, errors.New("object not found")
	}
	return s.obj.CutPayload(), nil
}

func TestDoubleWriteHeaderOnPayloadReadFailure(t *testing.T) {
	ctx := context.Background()
	addr := oidtest.Address()

	payloadSize := 1024 * 1024 // 1MB > chunk (256KB)
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	obj := generateObject(addr, nil, payload)

	readErr := errors.New("simulated payload read error")
	storage := &testStorageWithFailingReader{
		obj:       obj,
		failAfter: 300 * 1024, // > chunk
		err:       readErr,
	}

	anyNodeLists, nodeStrs := testNodeMatrix(t, []int{1})

	clientCache := &testClientCache{
		clients: make(map[string]*testClient),
	}
	remoteClient := newTestClient()
	remoteClient.addResult(addr, obj, nil)
	clientCache.clients[nodeStrs[0][0]] = remoteClient

	svc := &Service{cfg: new(cfg)}
	svc.log = zaptest.NewLogger(t)
	svc.localObjects = storage
	svc.localStorage = storage
	svc.clientCache = clientCache
	svc.neoFSNet = &testNeoFS{
		vectors: map[oid.Address][][]netmap.NodeInfo{
			addr: anyNodeLists,
		},
	}

	writer := &trackingWriter{}

	var prm Prm
	prm.SetObjectWriter(writer)
	prm.WithAddress(addr)
	prm.common = new(util.CommonPrm)

	err := svc.Get(ctx, prm)
	require.ErrorIs(t, err, readErr)

	t.Logf("WriteHeader called: %d times", writer.writeHeaderCount.Load())
	t.Logf("WriteChunk called: %d times", writer.writeChunkCount.Load())
	require.EqualValues(t, 1, writer.writeHeaderCount.Load())
}

func TestDoubleWriteHeaderOnChunkWriteFailure(t *testing.T) {
	ctx := context.Background()
	addr := oidtest.Address()

	payloadSize := 1024 * 1024 // 1MB > chunk (256KB)
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	obj := generateObject(addr, nil, payload)

	storage := newTestStorage()
	storage.addPhy(addr, obj)

	anyNodeLists, nodeStrs := testNodeMatrix(t, []int{1})

	clientCache := &testClientCache{
		clients: make(map[string]*testClient),
	}
	remoteClient := newTestClient()
	remoteClient.addResult(addr, obj, nil)
	clientCache.clients[nodeStrs[0][0]] = remoteClient

	svc := &Service{cfg: new(cfg)}
	svc.log = zaptest.NewLogger(t)
	svc.localObjects = storage
	svc.localStorage = storage
	svc.clientCache = clientCache
	svc.neoFSNet = &testNeoFS{
		vectors: map[oid.Address][][]netmap.NodeInfo{
			addr: anyNodeLists,
		},
	}

	writeErr := errors.New("simulated chunk write error")
	writer := &trackingWriter{
		failAfterChunks: 2,
		err:             writeErr,
	}

	var prm Prm
	prm.SetObjectWriter(writer)
	prm.WithAddress(addr)
	prm.common = new(util.CommonPrm)

	err := svc.Get(ctx, prm)
	require.ErrorIs(t, err, writeErr)

	t.Logf("WriteHeader called: %d times", writer.writeHeaderCount.Load())
	t.Logf("WriteChunk called: %d times", writer.writeChunkCount.Load())
	require.EqualValues(t, 1, writer.writeHeaderCount.Load())
}
