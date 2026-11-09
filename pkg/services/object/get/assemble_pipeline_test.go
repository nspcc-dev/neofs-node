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

	// retained so addNestedVirtualChild can rebuild the top-level link
	// after substituting one child's address below.
	ver      int
	srcObj   *object.Object
	children []*object.Object
	linkAddr oid.Address
}

const splitChildPayloadLen = 10

func buildSplitScene(t testing.TB, ver, n int, delay time.Duration) *splitScene {
	idCnr := cidtest.ID()

	ns, as := testNodeMatrix(t, []int{2})

	children, childIDs, payload := generateChain(n, idCnr)

	// srcObj's address is derived from its real content (rather than picked
	// upfront) so that the parent header embedded in children/link - which
	// readers must now verify actually hashes to the address they
	// requested - is genuinely self-consistent, matching how an honest
	// sender populates it.
	srcObj := new(object.Object)
	srcObj.SetContainerID(idCnr)
	srcObj.SetPayload(payload)
	srcObj.SetPayloadSize(uint64(len(payload)))
	require.NoError(t, srcObj.CalculateAndSetID())
	addr := srcObj.Address()

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

	return &splitScene{
		svc: svc, addr: addr, payload: payload, childIDs: childIDs, c1: c1, c2: c2,
		ver: ver, srcObj: srcObj, children: children, linkAddr: linkAddr,
	}
}

func (sc *splitScene) childAddr(i int) oid.Address {
	return oid.NewAddress(sc.addr.Container(), sc.childIDs[i])
}

func addNestedVirtualChild(t testing.TB, sc *splitScene, ver, idx int) {
	idCnr := sc.addr.Container()
	left := idx * splitChildPayloadLen
	payload := append([]byte(nil), sc.payload[left:left+splitChildPayloadLen]...)

	// Build the nested split's own parent from its real content first, and
	// derive its address from that (rather than reusing childIDs[idx], an
	// address picked upfront with no relation to this content) - same
	// reasoning as buildSplitScene. This makes childIDs[idx] - the id the
	// outer chain already uses to refer to this now-virtual child - change,
	// so every outer-chain structure that baked in the old value (the
	// top-level link, and the next child's previous-ID back-reference) is
	// patched to match below.
	parent := new(object.Object)
	parent.SetContainerID(idCnr)
	parent.SetPayload(payload)
	parent.SetPayloadSize(uint64(len(payload)))
	require.NoError(t, parent.CalculateAndSetID())
	parentAddr := parent.Address()

	oldChildID := sc.childIDs[idx]
	sc.childIDs[idx] = parentAddr.Object()
	if idx+1 < len(sc.children) {
		if prev := sc.children[idx+1].GetPreviousID(); prev == oldChildID {
			sc.children[idx+1].SetPreviousID(parentAddr.Object())
		}
	}

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
	vectors[parentAddr] = ns
	for i := range children {
		addr := oid.NewAddress(idCnr, childIDs[i])
		sc.c1.addResult(addr, nil, errors.New("any error"))
		sc.c2.addResult(addr, children[i], nil)
		vectors[addr] = ns
	}

	// The top-level link already baked in the old childIDs[idx] value (and
	// the old, now-replaced child object's payload size happens to be
	// unchanged, since both are exactly splitChildPayloadLen bytes) -
	// rebuild and re-register it so it reflects the new address.
	outerLinkObj, _ := buildSplitLink(sc.ver, sc.linkAddr, sc.srcObj, sc.children, sc.childIDs)
	sc.c2.addResult(sc.linkAddr, outerLinkObj, nil)
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
				addNestedVirtualChild(t, sc, sv.ver, 1)

				w, err := getFull(ctx, sc.svc, sc.addr)
				require.NoError(t, err)
				require.Equal(t, sc.payload, w.Object().Payload())
			})

			t.Run("nested virtual child payload only", func(t *testing.T) {
				setPrefetchWindow(t, 3)

				sc := buildSplitScene(t, sv.ver, 4, time.Millisecond)
				addNestedVirtualChild(t, sc, sv.ver, 1)

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

// foreignAttrKey marks a header built by foreignParentHeader, so its arrival
// in a response can be detected.
const foreignAttrKey = "injected"

// foreignParentHeader builds a well-formed header for an object other than the
// one sc requests. claimRequestedID makes it declare the requested object's ID,
// which its content does not hash to: the claimed-parent-address (isChild)
// checks then pass and only the content hash tells the two apart.
func foreignParentHeader(t testing.TB, sc *splitScene, claimRequestedID bool) *object.Object {
	foreign := new(object.Object)
	foreign.SetContainerID(sc.addr.Container())
	foreign.SetPayloadSize(uint64(len(sc.payload)))
	foreign.SetAttributes(object.NewAttribute(foreignAttrKey, "by-malicious-node"))
	require.NoError(t, foreign.CalculateAndSetID())
	require.NotEqual(t, sc.addr.Object(), foreign.GetID())

	if claimRequestedID {
		foreign.SetID(sc.addr.Object())
	}

	return foreign
}

// Assembly emits a parent header collected from a container node as the
// response header for the requested object. A node answering with a
// different object's header must not get that header through: the deferred
// eACL pass evaluates $Object filters against whatever fields it carries.
func TestAssembleSplitRejectsForeignParentHeader(t *testing.T) {
	ctx := context.Background()

	for _, sv := range []struct {
		name string
		ver  int
	}{
		{"V1", 1},
		{"V2", 2},
	} {
		for _, fc := range []struct {
			name             string
			claimRequestedID bool
		}{
			{name: "own ID"},
			{name: "claiming requested ID", claimRequestedID: true},
		} {
			t.Run(sv.name+"/"+fc.name, func(t *testing.T) {
				setPrefetchWindow(t, 3)

				sc := buildSplitScene(t, sv.ver, 4, time.Millisecond)
				foreign := foreignParentHeader(t, sc, fc.claimRequestedID)

				// Substitute it everywhere assembly could pick a parent
				// header up: the link object, and the last child of the chain.
				sc.c2.results[sc.linkAddr].obj.SetParent(foreign)
				sc.children[len(sc.children)-1].SetParent(foreign)

				w, err := getFull(ctx, sc.svc, sc.addr)

				// The read must not succeed. Which error surfaces differs by
				// split version and by which assembly path rejects first, so
				// assert the security property itself rather than a particular
				// error value: the attacker-chosen header must never reach the
				// response.
				require.Error(t, err)
				assertNoForeignHeader(t, w)
			})
		}
	}

	// processV2Last is reached only through a split info carrying no link, so
	// exercise it on its own: headChild reports the parent mismatch but still
	// returns the child, whose parent header is then used.
	t.Run("V2 last part", func(t *testing.T) {
		setPrefetchWindow(t, 3)

		sc := buildSplitScene(t, 2, 4, time.Millisecond)
		foreign := foreignParentHeader(t, sc, true)

		lastChild := sc.children[len(sc.children)-1]
		lastChild.SetParent(foreign)

		si := object.NewSplitInfo()
		si.SetFirstPart(sc.childIDs[0])
		si.SetLastPart(lastChild.GetID())
		sc.c2.addResult(sc.addr, nil, object.NewSplitInfoError(si))

		w, err := getFull(ctx, sc.svc, sc.addr)
		require.Error(t, err)
		assertNoForeignHeader(t, w)
	})
}

func assertNoForeignHeader(t *testing.T, w *SimpleObjectWriter) {
	for _, a := range w.Object().Attributes() {
		require.NotEqual(t, foreignAttrKey, a.Key(),
			"foreign parent header leaked into the response")
	}
}
