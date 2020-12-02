package getsvc

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
	b placement.Builder
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

func newTestStorage() *testStorage {
	return &testStorage{
		inhumed: make(map[string]struct{}),
		virtual: make(map[string]*objectSDK.SplitInfo),
		phy:     make(map[string]*object.Object),
	}
}

func (g *testTraverserGenerator) GenerateTraverser(addr *objectSDK.Address) (*placement.Traverser, error) {
	return placement.NewTraverser(
		placement.ForContainer(g.c),
		placement.ForObject(addr.ObjectID()),
		placement.UseBuilder(g.b),
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

func (c *testClientCache) get(_ *ecdsa.PrivateKey, addr string) (getClient, error) {
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

func (c *testClient) GetObject(_ context.Context, p Prm) (*objectSDK.Object, error) {
	v, ok := c.results[p.addr.String()]
	if !ok {
		return nil, object.ErrNotFound
	}

	return v.obj.Object().SDK(), v.err
}

func (c *testClient) head(_ context.Context, p Prm) (*object.Object, error) {
	v, ok := c.results[p.addr.String()]
	if !ok {
		return nil, object.ErrNotFound
	}

	if v.err != nil {
		return nil, v.err
	}

	return v.obj.CutPayload().Object(), nil
}

func (c *testClient) addResult(addr *objectSDK.Address, obj *object.RawObject, err error) {
	c.results[addr.String()] = struct {
		obj *object.RawObject
		err error
	}{obj: obj, err: err}
}

func (s *testStorage) Get(addr *objectSDK.Address) (*object.Object, error) {
	var (
		ok    bool
		obj   *object.Object
		sAddr = addr.String()
	)

	if _, ok = s.inhumed[sAddr]; ok {
		return nil, object.ErrAlreadyRemoved
	}

	if info, ok := s.virtual[sAddr]; ok {
		return nil, objectSDK.NewSplitInfoError(info)
	}

	if obj, ok = s.phy[sAddr]; ok {
		return obj, nil
	}

	return nil, object.ErrNotFound
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
		return Prm{
			objWriter: w,
			raw:       raw,
			common:    new(util.CommonPrm).WithLocalOnly(true),
		}
	}

	t.Run("OK", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		w := newSimpleObjectWriter()
		p := newPrm(false, w)

		addr := generateAddress()
		obj := generateObject(addr, nil, nil)

		storage.addPhy(addr, obj)

		p.addr = addr

		storage.addPhy(addr, obj)

		err := svc.Get(ctx, p)

		require.NoError(t, err)

		require.Equal(t, obj.Object(), w.object())
	})

	t.Run("INHUMED", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := generateAddress()

		storage.inhume(addr)

		p.addr = addr

		err := svc.Get(ctx, p)

		require.True(t, errors.Is(err, object.ErrAlreadyRemoved))
	})

	t.Run("404", func(t *testing.T) {
		storage := newTestStorage()
		svc := newSvc(storage)

		p := newPrm(false, nil)

		addr := generateAddress()

		p.addr = addr

		err := svc.Get(ctx, p)

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

		p.addr = addr

		storage.addVirtual(addr, splitInfo)

		err := svc.Get(ctx, p)

		errSplit := objectSDK.NewSplitInfoError(objectSDK.NewSplitInfo())

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
		svc.log = test.NewLogger(true)
		svc.localStorage = newTestStorage()
		svc.assembly = true
		svc.traverserGenerator = &testTraverserGenerator{
			c: cnr,
			b: b,
		}
		svc.clientCache = c

		return svc
	}

	newPrm := func(raw bool, w ObjectWriter) Prm {
		return Prm{
			objWriter: w,
			raw:       raw,
			common:    new(util.CommonPrm).WithLocalOnly(false),
		}
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

		obj := generateObject(addr, nil, nil)

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

		w := newSimpleObjectWriter()

		p := newPrm(false, w)
		p.addr = addr

		err := svc.Get(ctx, p)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), w.object())

		*c1, *c2 = *c2, *c1

		err = svc.Get(ctx, p)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), w.object())
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
		p.addr = addr

		err := svc.Get(ctx, p)
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
		p.addr = addr

		err := svc.Get(ctx, p)
		require.True(t, errors.Is(err, object.ErrNotFound))
	})

	t.Run("VIRTUAL", func(t *testing.T) {
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

				p := newPrm(false, nil)
				p.addr = addr

				err := svc.Get(ctx, p)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)

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
				c2.addResult(child2Addr, nil, errors.New("any error"))

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

				p := newPrm(false, newSimpleObjectWriter())
				p.addr = addr

				err := svc.Get(ctx, p)
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

				w := newSimpleObjectWriter()

				p := newPrm(false, w)
				p.addr = addr

				err := svc.Get(ctx, p)
				require.NoError(t, err)
				require.Equal(t, srcObj.Object(), w.object())
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

				p := newPrm(false, nil)
				p.addr = addr

				err := svc.Get(ctx, p)
				require.True(t, errors.Is(err, object.ErrNotFound))
			})

			t.Run("get chain element failure", func(t *testing.T) {
				addr := generateAddress()
				addr.SetContainerID(cid)
				addr.SetObjectID(generateID())

				srcObj := generateObject(addr, nil, nil)

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
						preRightAddr.String(): ns,
					},
				}

				svc := newSvc(builder, &testClientCache{
					clients: map[string]*testClient{
						as[0][0]: c1,
						as[0][1]: c2,
					},
				})

				headSvc := newTestClient()
				headSvc.addResult(preRightAddr, nil, object.ErrNotFound)
				svc.headSvc = headSvc

				p := newPrm(false, newSimpleObjectWriter())
				p.addr = addr

				err := svc.Get(ctx, p)
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

				svc.headSvc = c2

				w := newSimpleObjectWriter()

				p := newPrm(false, w)
				p.addr = addr

				err := svc.Get(ctx, p)
				require.NoError(t, err)
				require.Equal(t, srcObj.Object(), w.object())
			})
		})
	})
}
