package getsvc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type splitScene struct {
	svc      *Service
	addr     oid.Address
	payload  []byte
	childIDs []oid.ID
	c1, c2   *testClient
}

const splitChildPayloadLen = 10

func buildSplitScene(t testing.TB, ver, n int, delay time.Duration) *splitScene {
	idCnr := cidtest.ID()

	addr := oidtest.Address()
	addr.SetContainer(idCnr)
	addr.SetObject(oidtest.ID())

	srcObj := generateObject(addr, nil, nil)

	ns, as := testNodeMatrix(t, []int{2})

	children, childIDs, payload := generateChain(n, idCnr)
	srcObj.SetPayload(payload)
	srcObj.SetPayloadSize(uint64(len(payload)))
	children[len(children)-1].SetParent(srcObj)

	linkID := oidtest.ID()
	linkAddr := oid.NewAddress(idCnr, linkID)
	linkObj, splitInfo := buildSplitLink(ver, linkAddr, srcObj, children, childIDs)

	c1 := newTestClient()
	c2 := newTestClient()
	c2.delay = delay

	c1.addResult(addr, nil, errors.New("any error"))
	c2.addResult(addr, nil, object.NewSplitInfoError(splitInfo))

	c1.addResult(linkAddr, nil, errors.New("any error"))
	c2.addResult(linkAddr, linkObj, nil)

	vectors := map[oid.Address][][]netmap.NodeInfo{
		addr:     ns,
		linkAddr: ns,
	}

	for i := range children {
		var childAddr oid.Address
		childAddr.SetContainer(idCnr)
		childAddr.SetObject(childIDs[i])
		c1.addResult(childAddr, nil, errors.New("any error"))
		c2.addResult(childAddr, children[i], nil)
		vectors[childAddr] = ns
	}

	svc := &Service{cfg: new(cfg)}
	svc.log = zap.NewNop()
	svc.localStorage = newTestStorage()
	svc.neoFSNet = &testNeoFS{vectors: vectors}
	svc.clientCache = &testClientCache{
		clients: map[string]*testClient{
			as[0][0]: c1,
			as[0][1]: c2,
		},
	}

	return &splitScene{svc: svc, addr: addr, payload: payload, childIDs: childIDs, c1: c1, c2: c2}
}

func (sc *splitScene) childAddr(i int) oid.Address {
	return oid.NewAddress(sc.addr.Container(), sc.childIDs[i])
}

func addNestedVirtualChild(sc *splitScene, ver, idx int) {
	idCnr := sc.addr.Container()
	parentAddr := sc.childAddr(idx)
	left := idx * splitChildPayloadLen
	payload := append([]byte(nil), sc.payload[left:left+splitChildPayloadLen]...)

	parent := generateObject(parentAddr, nil, payload)
	parent.SetPayloadSize(uint64(len(payload)))

	childIDs := []oid.ID{oidtest.ID(), oidtest.ID()}
	children := make([]*object.Object, len(childIDs))
	var prev *oid.ID
	for i, id := range childIDs {
		part := append([]byte(nil), payload[i*5:(i+1)*5]...)
		addr := oid.NewAddress(idCnr, id)
		children[i] = generateObject(addr, prev, part)
		children[i].SetParentID(parentAddr.Object())
		if i == len(childIDs)-1 {
			children[i].SetParent(parent)
		}
		cp := id
		prev = &cp
	}

	linkID := oidtest.ID()
	linkAddr := oid.NewAddress(idCnr, linkID)
	linkObj, splitInfo := buildSplitLink(ver, linkAddr, parent, children, childIDs)

	sc.c1.addResult(parentAddr, nil, errors.New("any error"))
	sc.c2.addResult(parentAddr, nil, object.NewSplitInfoError(splitInfo))
	sc.c1.addResult(linkAddr, nil, errors.New("any error"))
	sc.c2.addResult(linkAddr, linkObj, nil)

	vectors := sc.svc.neoFSNet.(*testNeoFS).vectors
	ns := vectors[sc.addr]
	vectors[linkAddr] = ns
	for i := range children {
		addr := oid.NewAddress(idCnr, childIDs[i])
		sc.c1.addResult(addr, nil, errors.New("any error"))
		sc.c2.addResult(addr, children[i], nil)
		vectors[addr] = ns
	}
}

func buildSplitLink(ver int, linkAddr oid.Address, parent *object.Object, children []*object.Object, childIDs []oid.ID) (*object.Object, *object.SplitInfo) {
	splitInfo := object.NewSplitInfo()
	splitInfo.SetLink(linkAddr.Object())

	var linkObj *object.Object
	switch ver {
	case 1:
		splitInfo.SetSplitID(object.NewSplitID())
		linkObj = generateObject(linkAddr, nil, nil, childIDs...)
	case 2:
		splitInfo.SetFirstPart(childIDs[0])
		linkObj = generateObject(linkAddr, nil, nil)
		linkObj.SetFirstID(childIDs[0])

		linkChildren := make([]object.MeasuredObject, len(children))
		for i := range children {
			children[i].SetParentID(parent.GetID())
			linkChildren[i].SetObjectID(children[i].GetID())
			linkChildren[i].SetObjectSize(uint32(children[i].PayloadSize()))
		}

		var link object.Link
		link.SetObjects(linkChildren)
		linkObj.WriteLink(link)
	}
	linkObj.SetParentID(parent.GetID())
	linkObj.SetParent(parent)

	return linkObj, splitInfo
}

func getInto(ctx context.Context, svc *Service, addr oid.Address, w ObjectWriter) error {
	var p Prm
	p.SetObjectWriter(w)
	p.common = new(util.CommonPrm).WithLocalOnly(false)
	p.WithAddress(addr)
	return svc.Get(ctx, p)
}

func getFull(ctx context.Context, svc *Service, addr oid.Address) (*SimpleObjectWriter, error) {
	w := NewSimpleObjectWriter()
	return w, getInto(ctx, svc, addr, w)
}

func setPrefetchWindow(t testing.TB, w int) {
	old := prefetchWindow
	prefetchWindow = w
	t.Cleanup(func() { prefetchWindow = old })
}

func TestAssembleSplitPipelined(t *testing.T) {
	ctx := context.Background()

	for _, sv := range []struct {
		name string
		ver  int
	}{
		{"V1", 1},
		{"V2", 2},
	} {
		t.Run(sv.name, func(t *testing.T) {
			t.Run("ordered output across windows", func(t *testing.T) {
				for _, win := range []int{1, 2, 3, 8} {
					t.Run(fmt.Sprintf("window=%d", win), func(t *testing.T) {
						setPrefetchWindow(t, win)

						sc := buildSplitScene(t, sv.ver, 8, 2*time.Millisecond)

						w, err := getFull(ctx, sc.svc, sc.addr)
						require.NoError(t, err)
						require.Equal(t, sc.payload, w.Object().Payload())
					})
				}
			})

			t.Run("remote failure discards later children", func(t *testing.T) {
				const n = 6
				const failIdx = 3

				sc := buildSplitScene(t, sv.ver, n, time.Millisecond)
				sc.c2.addResult(sc.childAddr(failIdx), nil, apistatus.ErrObjectNotFound)

				w, err := getFull(ctx, sc.svc, sc.addr)
				require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
				require.Equal(t, sc.payload[:failIdx*splitChildPayloadLen], w.Object().Payload())
			})

			t.Run("virtual child is reported, not assembled recursively", func(t *testing.T) {
				setPrefetchWindow(t, 3)

				const failIdx = 1
				sc := buildSplitScene(t, sv.ver, 6, time.Millisecond)
				sc.c2.addResult(sc.childAddr(failIdx), nil, object.NewSplitInfoError(object.NewSplitInfo()))

				w, err := getFull(ctx, sc.svc, sc.addr)
				var splitErr *object.SplitInfoError
				require.ErrorAs(t, err, &splitErr)
				require.Equal(t, sc.payload[:failIdx*splitChildPayloadLen], w.Object().Payload())
			})

			t.Run("client write failure aborts cleanly", func(t *testing.T) {
				setPrefetchWindow(t, 4)

				sc := buildSplitScene(t, sv.ver, 8, 2*time.Millisecond)
				w := &trackingWriter{failAfterChunks: 3, err: errors.New("client gone")}

				require.Error(t, getInto(ctx, sc.svc, sc.addr, w))
			})

			t.Run("context cancel mid-stream", func(t *testing.T) {
				sc := buildSplitScene(t, sv.ver, 16, 20*time.Millisecond)

				cctx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(30 * time.Millisecond)
					cancel()
				}()

				_, err := getFull(cctx, sc.svc, sc.addr)
				require.Error(t, err)
			})

			t.Run("nested virtual child", func(t *testing.T) {
				setPrefetchWindow(t, 3)

				sc := buildSplitScene(t, sv.ver, 4, time.Millisecond)
				addNestedVirtualChild(sc, sv.ver, 1)

				w, err := getFull(ctx, sc.svc, sc.addr)
				require.NoError(t, err)
				require.Equal(t, sc.payload, w.Object().Payload())
			})

			t.Run("nested virtual child payload only", func(t *testing.T) {
				setPrefetchWindow(t, 3)

				sc := buildSplitScene(t, sv.ver, 4, time.Millisecond)
				addNestedVirtualChild(sc, sv.ver, 1)

				w := NewSimpleObjectWriter()
				var p Prm
				p.SetObjectWriter(w)
				p.MarkPayloadOnly()
				p.RequireEACLRecheck()
				p.common = new(util.CommonPrm).WithLocalOnly(false)
				p.WithAddress(sc.addr)

				require.NoError(t, sc.svc.Get(ctx, p))
				require.Equal(t, sc.payload, w.Object().Payload())
			})
		})

		t.Run("V2 accepts zero parent object ID", func(t *testing.T) {
			setPrefetchWindow(t, 3)

			sc := buildSplitScene(t, 2, 4, time.Millisecond)
			zeroParent := new(object.Object)
			zeroParent.SetContainerID(sc.addr.Container())
			zeroParent.SetPayloadSize(uint64(len(sc.payload)))

			for i := range sc.childIDs {
				child := sc.c2.results[sc.childAddr(i)].obj
				child.SetParent(zeroParent)
			}

			w, err := getFull(ctx, sc.svc, sc.addr)
			require.NoError(t, err)
			require.Equal(t, sc.payload, w.Object().Payload())
		})
	}
}

func BenchmarkAssembleSplitObject(b *testing.B) {
	ctx := context.Background()

	for _, win := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("window=%d", win), func(b *testing.B) {
			setPrefetchWindow(b, win)

			sc := buildSplitScene(b, 1, 16, time.Millisecond)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := getFull(ctx, sc.svc, sc.addr)
				require.NoError(b, err)
			}
		})
	}
}
