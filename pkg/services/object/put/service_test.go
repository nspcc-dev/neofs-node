package putsvc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	currentEpoch  = 123
	maxObjectSize = 1000
)

func Test_Slicing_Small_REP3(t *testing.T) {
	const repNodes = 3
	const cnrReserveNodes = 2
	const outCnrNodes = 2

	cluster := newTestClusterForRepPolicy(t, repNodes, cnrReserveNodes, outCnrNodes)

	var obj object.Object
	obj.SetContainerID(cidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetAttributes(
		object.NewAttribute("attr1", "val1"),
		object.NewAttribute("attr2", "val2"),
	)

	var sessionToken session.Object
	sessionToken.SetID(uuid.New())
	sessionToken.SetExp(1)
	sessionToken.BindContainer(cidtest.ID())

	testThroughNode := func(t *testing.T, idx int) {
		sessionToken.SetAuthKey(cluster.nodeSessions[idx].signer.Public())
		require.NoError(t, sessionToken.Sign(usertest.User()))

		storeObjectWithSession(t, cluster.nodeServices[idx], obj, sessionToken)

		// check objects are place according to policy
		nodeObjLists := slices.Collect(cluster.allStoredObjects())
		for i := range nodeObjLists {
			if i < repNodes {
				require.Len(t, nodeObjLists[i], 1, i)
			} else {
				require.Empty(t, nodeObjLists[i], i)
			}
		}

		storedObj := nodeObjLists[0][0]

		// check replica is correct
		assertObjectIntegrity(t, storedObj)
		require.Equal(t, sessionToken, *storedObj.SessionToken())
		require.Equal(t, obj.GetContainerID(), storedObj.GetContainerID())
		require.Equal(t, sessionToken.Issuer(), storedObj.Owner())
		require.EqualValues(t, currentEpoch, storedObj.CreationEpoch())
		require.Zero(t, storedObj.Type())
		require.Equal(t, obj.Attributes(), storedObj.Attributes())
		require.False(t, storedObj.HasParent())
		require.True(t, bytes.Equal(obj.Payload(), storedObj.Payload()))

		// check all replicas are the same
		for i := 1; i < repNodes; i++ {
			obj := nodeObjLists[i][0]
			// require.Equal(t, storedObjs[0], obj) can fail for empty payload because []byte{} != []byte(nil)
			require.Equal(t, storedObj.CutPayload(), obj.CutPayload())
			require.True(t, bytes.Equal(storedObj.Payload(), obj.Payload()))
		}

		cluster.resetAllStoredObjects()
	}

	testPayloadLen := func(t *testing.T, ln int) {
		obj.SetPayload(testutil.RandByteSlice(ln))
		t.Run("through replica holders", func(t *testing.T) {
			for i := range repNodes + cnrReserveNodes {
				testThroughNode(t, i)
			}
		})
		t.Run("through reserve nodes", func(t *testing.T) {
			for i := range cnrReserveNodes {
				testThroughNode(t, repNodes+i)
			}
		})
		t.Run("through non-container nodes", func(t *testing.T) {
			for i := range outCnrNodes {
				testThroughNode(t, repNodes+cnrReserveNodes+i)
			}
		})
	}

	t.Run("no payload", func(t *testing.T) {
		testPayloadLen(t, 0)
	})
	t.Run("1B", func(t *testing.T) {
		testPayloadLen(t, 1)
	})
	t.Run("limit-1B", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize-1)
	})
	t.Run("exactly limit", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize)
	})
}

func Test_Slicing_Big_REP3(t *testing.T) {
	const repNodes = 3
	const cnrReserveNodes = 2
	const outCnrNodes = 2

	cluster := newTestClusterForRepPolicy(t, repNodes, cnrReserveNodes, outCnrNodes)

	var obj object.Object
	obj.SetContainerID(cidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetAttributes(
		object.NewAttribute("attr1", "val1"),
		object.NewAttribute("attr2", "val2"),
	)

	var sessionToken session.Object
	sessionToken.SetID(uuid.New())
	sessionToken.SetExp(1)
	sessionToken.BindContainer(cidtest.ID())

	testThroughNode := func(t *testing.T, ln, idx int) {
		obj.SetPayload(testutil.RandByteSlice(ln))

		sessionToken.SetAuthKey(cluster.nodeSessions[idx].signer.Public())
		require.NoError(t, sessionToken.Sign(usertest.User()))

		storeObjectWithSession(t, cluster.nodeServices[idx], obj, sessionToken)

		// check objects are place according to policy
		nodeObjLists := slices.Collect(cluster.allStoredObjects())
		for i := range nodeObjLists {
			if i < repNodes {
				assertSplitChain(t, maxObjectSize, obj, sessionToken, nodeObjLists[i])
			} else {
				require.Empty(t, nodeObjLists[i], i)
			}
		}

		// check all replicas are the same
		for i := 1; i < repNodes; i++ {
			require.Equal(t, nodeObjLists[0], nodeObjLists[i])
		}

		cluster.resetAllStoredObjects()
	}

	testPayloadLen := func(t *testing.T, ln int) {
		t.Run("through replica holders", func(t *testing.T) {
			for i := range repNodes + cnrReserveNodes {
				testThroughNode(t, ln, i)
			}
		})
		t.Run("through reserve nodes", func(t *testing.T) {
			for i := range cnrReserveNodes {
				testThroughNode(t, ln, repNodes+i)
			}
		})
		t.Run("through non-container nodes", func(t *testing.T) {
			for i := range outCnrNodes {
				testThroughNode(t, ln, repNodes+cnrReserveNodes+i)
			}
		})
	}

	t.Run("limit+1B", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize+1)
	})
	t.Run("limitX2", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize*2)
	})
	t.Run("limitX4-1", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize*4-1)
	})
	t.Run("limitX5", func(t *testing.T) {
		testPayloadLen(t, maxObjectSize*5)
	})
}

func newTestClusterForRepPolicy(t *testing.T, repNodes, cnrReserveNodes, outCnrNodes uint) *testCluster {
	allNodes := allocNodes([]uint{repNodes + cnrReserveNodes + outCnrNodes})[0]
	cnrNodes := allNodes[:repNodes+cnrReserveNodes]

	cn := mockContainerNodes{
		unsorted:  [][]netmap.NodeInfo{cnrNodes},
		sorted:    [][]netmap.NodeInfo{cnrNodes},
		repCounts: []uint{repNodes},
	}

	cluster := testCluster{
		nodeServices:      make(nodeServices, len(allNodes)),
		nodeNetworks:      make([]mockNetwork, len(allNodes)),
		nodeSessions:      make([]mockNodeSession, len(allNodes)),
		nodeLocalStorages: make([]inMemLocalStorage, len(allNodes)),
	}

	for i := range allNodes {
		nodeKey := neofscryptotest.ECDSAPrivateKey()

		nodeWorkerPool, err := ants.NewPool(10, ants.WithNonblocking(true))
		require.NoError(t, err)

		cluster.nodeNetworks[i] = mockNetwork{
			mockNodeState: mockNodeState{
				epoch: currentEpoch,
			},
			localPub: allNodes[i].PublicKey(),
			cnrNodes: cn,
		}

		cluster.nodeSessions[i] = mockNodeSession{
			signer:    neofscryptotest.Signer(),
			expiresAt: cluster.nodeNetworks[i].mockNodeState.epoch + 1,
		}

		cluster.nodeServices[i] = NewService(cluster.nodeServices, &cluster.nodeNetworks[i], nil,
			WithLogger(zaptest.NewLogger(t).With(zap.Int("node", i))),
			WithKeyStorage(objutil.NewKeyStorage(&nodeKey, cluster.nodeSessions[i], &cluster.nodeNetworks[i])),
			WithObjectStorage(&cluster.nodeLocalStorages[i]),
			WithMaxSizeSource(mockMaxSize(maxObjectSize)),
			WithContainerSource(mockContainer{}),
			WithNetworkState(&cluster.nodeNetworks[i]),
			WithClientConstructor(cluster.nodeServices),
			WithSplitChainVerifier(mockSplitVerifier{}),
			WithRemoteWorkerPool(nodeWorkerPool),
		)
	}

	return &cluster
}

type mockSplitVerifier struct{}

func (mockSplitVerifier) VerifySplit(context.Context, cid.ID, oid.ID, []object.MeasuredObject) error {
	return nil
}

type mockContainer container.Container

func (x mockContainer) Get(cid.ID) (container.Container, error) {
	return container.Container(x), nil
}

type mockNetwork struct {
	mockNodeState
	localPub []byte
	cnrNodes mockContainerNodes
}

func (*mockNetwork) CurrentBlock() uint32 {
	panic("unimplemented")
}

func (*mockNetwork) CurrentEpochDuration() uint64 {
	panic("unimplemented")
}

func (x *mockNetwork) GetContainerNodes(cid.ID) (ContainerNodes, error) {
	return x.cnrNodes, nil
}

func (x *mockNetwork) IsLocalNodePublicKey(pub []byte) bool {
	return bytes.Equal(x.localPub, pub)
}

func (*mockNetwork) GetEpochBlock(uint64) (uint32, error) {
	panic("unimplemented")
}

type mockContainerNodes struct {
	unsorted  [][]netmap.NodeInfo
	sorted    [][]netmap.NodeInfo
	repCounts []uint
}

func (x mockContainerNodes) Unsorted() [][]netmap.NodeInfo {
	return x.unsorted
}

func (x mockContainerNodes) SortForObject(oid.ID) ([][]netmap.NodeInfo, error) {
	return x.sorted, nil
}

func (x mockContainerNodes) PrimaryCounts() []uint {
	return x.repCounts
}

type mockMaxSize uint64

func (x mockMaxSize) MaxObjectSize() uint64 {
	return uint64(x)
}

type mockNodeSession struct {
	signer    neofscryptotest.VariableSigner
	expiresAt uint64
}

func (x mockNodeSession) Get(user.ID, []byte) *storage.PrivateToken {
	return storage.NewPrivateToken(&x.signer.ECDSAPrivateKey, x.expiresAt)
}

type mockNodeState struct {
	epoch uint64
}

func (x mockNodeState) CurrentEpoch() uint64 {
	return x.epoch
}

type inMemLocalStorage struct {
	mtx  sync.RWMutex
	objs []object.Object
}

func (x *inMemLocalStorage) Put(obj *object.Object, objBin []byte) error {
	if objBin != nil {
		got := obj.Marshal()
		if len(obj.Payload()) == 0 && len(objBin) == len(got)+2 {
			// this may happen because encodeReplicateRequestWithoutPayload, unlike Marshal,
			// adds field tag for payload even when it is empty
			objBin = objBin[:len(got)]
		}
		if !bytes.Equal(objBin, got) {
			return errors.New("binary mismatches object")
		}
	}

	x.mtx.Lock()
	x.objs = append(x.objs, *obj)
	x.mtx.Unlock()

	return nil
}

func (x *inMemLocalStorage) Delete(oid.Address, uint64, []oid.ID) error {
	panic("unimplemented")
}

func (x *inMemLocalStorage) Lock(oid.Address, []oid.ID) error {
	panic("unimplemented")
}

func (x *inMemLocalStorage) IsLocked(oid.Address) (bool, error) {
	panic("unimplemented")
}

type nodeServices []*Service

func (x nodeServices) lookupNode(node clientcore.NodeInfo) (*Service, error) {
	ind := slices.IndexFunc(x, func(svc *Service) bool {
		return svc.neoFSNet.IsLocalNodePublicKey(node.PublicKey())
	})
	if ind < 0 {
		return nil, errors.New("unknown node")
	}
	return x[ind], nil
}

func (x nodeServices) Get(node clientcore.NodeInfo) (clientcore.MultiAddressClient, error) {
	svc, err := x.lookupNode(node)
	if err != nil {
		return nil, err
	}
	return (*serviceClient)(svc), nil
}

func (x nodeServices) SendReplicationRequestToNode(_ context.Context, reqBin []byte, node clientcore.NodeInfo) ([]byte, error) {
	var req protoobject.ReplicateRequest
	if err := proto.Unmarshal(reqBin, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.Object == nil {
		return nil, errors.New("missing object in request")
	}

	var obj object.Object
	if err := obj.FromProtoMessage(req.Object); err != nil {
		return nil, fmt.Errorf("invalid object in request: %w", err)
	}

	svc, err := x.lookupNode(node)
	if err != nil {
		return nil, err
	}

	if err := svc.ValidateAndStoreObjectLocally(obj); err != nil {
		return nil, fmt.Errorf("validate and store object locally: %w", err)
	}

	return nil, nil
}

type serviceClient Service

func (m *serviceClient) ContainerAnnounceUsedSpace(context.Context, []container.SizeEstimation, client.PrmAnnounceSpace) error {
	panic("unimplemented")
}

func (m *serviceClient) ObjectPutInit(ctx context.Context, hdr object.Object, _ user.Signer, _ client.PrmObjectPutInit) (client.ObjectWriter, error) {
	stream, err := (*Service)(m).Put(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: following is needed because struct parameters privatize some data. Refactor to avoid this.
	localReq := &protoobject.PutRequest{
		MetaHeader: &protosession.RequestMetaHeader{Ttl: 1},
	}
	commonPrm, err := objutil.CommonPrmFromRequest(localReq)
	if err != nil {
		panic(err)
	}

	var ip PutInitPrm
	ip.WithObject(hdr.CutPayload())
	ip.WithCommonPrm(commonPrm)

	if err := stream.Init(&ip); err != nil {
		return nil, err
	}

	return (*testPayloadStream)(stream), nil
}

func (m *serviceClient) ReplicateObject(context.Context, oid.ID, io.ReadSeeker, neofscrypto.Signer, bool) (*neofscrypto.Signature, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectDelete(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectDelete) (oid.ID, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectGetInit(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectGet) (object.Object, *client.PayloadReader, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectHead(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectHead) (*object.Object, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectSearchInit(context.Context, cid.ID, user.Signer, client.PrmObjectSearch) (*client.ObjectListReader, error) {
	panic("unimplemented")
}

func (m *serviceClient) SearchObjects(context.Context, cid.ID, object.SearchFilters, []string, string, neofscrypto.Signer, client.SearchObjectsOptions) ([]client.SearchResultItem, string, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectRangeInit(context.Context, cid.ID, oid.ID, uint64, uint64, user.Signer, client.PrmObjectRange) (*client.ObjectRangeReader, error) {
	panic("unimplemented")
}

func (m *serviceClient) ObjectHash(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectHash) ([][]byte, error) {
	panic("unimplemented")
}

func (m *serviceClient) AnnounceLocalTrust(context.Context, uint64, []apireputation.Trust, client.PrmAnnounceLocalTrust) error {
	// TODO: interfaces are oversaturated. This will never be needed to server object PUT. Refactor this.
	panic("unimplemented")
}

func (m *serviceClient) AnnounceIntermediateTrust(context.Context, uint64, apireputation.PeerToPeerTrust, client.PrmAnnounceIntermediateTrust) error {
	panic("unimplemented")
}

func (m *serviceClient) ForEachGRPCConn(context.Context, func(context.Context, *grpc.ClientConn) error) error {
	panic("unimplemented")
}

type testPayloadStream Streamer

func (x *testPayloadStream) Write(p []byte) (int, error) {
	if err := (*Streamer)(x).SendChunk(new(PutChunkPrm).WithChunk(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (x *testPayloadStream) Close() error {
	_, err := (*Streamer)(x).Close()
	return err
}

func (testPayloadStream) GetResult() client.ResObjectPut {
	return client.ResObjectPut{}
}

type testCluster struct {
	nodeServices      nodeServices
	nodeNetworks      []mockNetwork
	nodeSessions      []mockNodeSession
	nodeLocalStorages []inMemLocalStorage
}

func (x testCluster) allStoredObjects() iter.Seq[[]object.Object] {
	return func(yield func([]object.Object) bool) {
		for i := range x.nodeLocalStorages {
			if !yield(x.nodeLocalStorages[i].objs) {
				return
			}
		}
	}
}

func (x *testCluster) resetAllStoredObjects() {
	for i := range x.nodeLocalStorages {
		x.nodeLocalStorages[i].objs = nil
	}
}

func storeObjectWithSession(t *testing.T, svc *Service, obj object.Object, st session.Object) {
	stream, err := svc.Put(context.Background())
	require.NoError(t, err)

	req := &protoobject.PutRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			Ttl:          2,
			SessionToken: st.ProtoMessage(),
		},
	}

	commonPrm, err := objutil.CommonPrmFromRequest(req)
	require.NoError(t, err)

	ip := new(PutInitPrm).
		WithObject(obj.CutPayload()).
		WithCommonPrm(commonPrm)
	require.NoError(t, stream.Init(ip))

	cp := new(PutChunkPrm).
		WithChunk(obj.Payload())
	require.NoError(t, stream.SendChunk(cp))

	_, err = stream.Close()
	require.NoError(t, err)
}

func assertObjectIntegrity(t *testing.T, obj object.Object) {
	require.NoError(t, obj.CheckVerificationFields())
	require.NotNil(t, obj.Version())
	require.Equal(t, version.Current(), *obj.Version())
	require.NotZero(t, obj.GetContainerID())
	require.NotZero(t, obj.Owner())

	payload := obj.Payload()
	require.EqualValues(t, obj.PayloadSize(), len(payload))

	cs, ok := obj.PayloadChecksum()
	require.True(t, ok)
	require.Equal(t, checksum.SHA256, cs.Type())
	got := sha256.Sum256(payload)
	require.Equal(t, got[:], cs.Value())

	if cs, ok := obj.PayloadHomomorphicHash(); ok {
		require.Equal(t, checksum.TillichZemor, cs.Type())
		got := tz.Sum(payload)
		require.Equal(t, got[:], cs.Value())
	}
}

func assertSplitChain(t *testing.T, limit int, src object.Object, sessionToken session.Object, members []object.Object) {
	payload := src.Payload()
	require.Len(t, members, splitMembersCount(limit, len(payload)))

	for _, member := range members {
		assertObjectIntegrity(t, member)
		require.Equal(t, src.Version(), member.Version())
		require.Equal(t, sessionToken, *member.SessionToken())
		require.Equal(t, src.GetContainerID(), member.GetContainerID())
		require.Equal(t, sessionToken.Issuer(), member.Owner())
		require.EqualValues(t, currentEpoch, member.CreationEpoch())
		require.Equal(t, src.Attributes(), member.Attributes())
		require.True(t, member.HasParent())
		require.Empty(t, member.Children())
	}

	chunks, linker := members[:len(members)-1], members[len(members)-1]

	var gotPayload []byte
	for i := range chunks {
		require.Equal(t, src.Type(), chunks[i].Type())
		gotPayload = append(gotPayload, members[i].Payload()...)
	}
	require.True(t, bytes.Equal(gotPayload, payload))

	require.Zero(t, chunks[0].GetFirstID())
	require.Zero(t, chunks[0].GetPreviousID())
	for i := 1; i < len(chunks); i++ {
		require.Equal(t, chunks[0].GetID(), chunks[i].GetFirstID())
		require.Equal(t, chunks[i-1].GetID(), chunks[i].GetPreviousID())
	}

	require.Equal(t, object.TypeLink, linker.Type())
	require.Equal(t, chunks[0].GetID(), linker.GetFirstID())

	// TODO: check everything possible
}

func splitMembersCount(limit, ln int) int {
	if ln <= limit {
		return 1
	}
	res := ln/limit + 1 // + LINK
	if ln%limit != 0 {
		res++
	}
	return res
}
