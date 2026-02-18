package putsvc

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/reedsolomon"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	storage "github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	containertest "github.com/nspcc-dev/neofs-sdk-go/container/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	netmaptest "github.com/nspcc-dev/neofs-sdk-go/netmap/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	currentEpoch  = 123
	maxObjectSize = 1000
)

type quotas struct {
	soft, hard uint64
}

func (q quotas) AvailableQuotasLeft(cID cid.ID, owner user.ID) (uint64, uint64, error) {
	return q.soft, q.hard, nil
}

type payments struct {
	m map[cid.ID]int64
}

func (p *payments) UnpaidSince(cID cid.ID) (unpaidFromEpoch int64, err error) {
	if p.m == nil {
		return -1, nil
	}

	e, ok := p.m[cID]
	if !ok {
		return -1, nil
	}

	return e, nil
}

func TestPayments(t *testing.T) {
	var (
		cluster           = newTestClusterForRepPolicy(t, 1, 1, 1)
		nodeKey           = neofscryptotest.ECDSAPrivateKey()
		cnr               = containertest.Container()
		rep               = netmap.ReplicaDescriptor{}
		p                 = netmaptest.PlacementPolicy()
		nodeWorkerPool, _ = ants.NewPool(1, ants.WithNonblocking(true))
		cID               = cidtest.ID()
		owner             = usertest.User()
		payments          = &payments{map[cid.ID]int64{cID: 123}}
	)

	rep.SetNumberOfObjects(1)
	p.SetReplicas([]netmap.ReplicaDescriptor{rep})
	cnr.SetPlacementPolicy(p)

	s := NewService(cluster.nodeServices, &cluster.nodeNetworks[0], nil,
		quotas{math.MaxUint64, math.MaxUint64},
		payments,
		WithLogger(zaptest.NewLogger(t)),
		WithKeyStorage(objutil.NewKeyStorage(&nodeKey, cluster.nodeSessions[0], &cluster.nodeNetworks[0])),
		WithObjectStorage(&cluster.nodeLocalStorages[0]),
		WithMaxSizeSource(mockMaxSize(maxObjectSize)),
		WithContainerSource(mockContainer(cnr)),
		WithNetworkState(&cluster.nodeNetworks[0]),
		WithClientConstructor(cluster.nodeServices),
		WithSplitChainVerifier(mockSplitVerifier{}),
		WithRemoteWorkerPool(nodeWorkerPool),
	)

	t.Run("session v1", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		var sessionToken session.Object
		sessionToken.SetID(uuid.New())
		sessionToken.SetExp(1)
		sessionToken.BindContainer(cID)
		sessionToken.SetAuthKey(cluster.nodeSessions[0].signer.Public())
		require.NoError(t, sessionToken.Sign(owner))

		req := &protoobject.PutRequest{
			MetaHeader: &protosession.RequestMetaHeader{
				Ttl:          2,
				SessionToken: sessionToken.ProtoMessage(),
			},
		}
		commonPrm, err := objutil.CommonPrmFromRequest(req)
		if err != nil {
			panic(err)
		}

		o := objecttest.Object()
		o.SetContainerID(cID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)
		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrm)

		err = stream.Init(ip)
		require.ErrorContains(t, err, "container is unpaid")

		payments.m[cID] = -1

		require.NoError(t, stream.Init(ip))

		payments.m[cID] = 123 // reset for next test
	})

	t.Run("session v2", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		sessionTokenV2 := newSessionTokenV2(t, cID, owner, cluster.nodeSessions, []sessionv2.Verb{sessionv2.VerbObjectPut})

		req := &protoobject.PutRequest{
			MetaHeader: &protosession.RequestMetaHeader{
				Ttl:            2,
				SessionTokenV2: sessionTokenV2.ProtoMessage(),
			},
		}
		commonPrm, err := objutil.CommonPrmFromRequest(req)
		if err != nil {
			panic(err)
		}

		o := objecttest.Object()
		o.SetContainerID(cID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)
		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrm)

		err = stream.Init(ip)
		require.ErrorContains(t, err, "container is unpaid")

		payments.m[cID] = -1

		require.NoError(t, stream.Init(ip))
	})
}

func TestQuotas(t *testing.T) {
	const hardLimit = 2
	var (
		nodeKey = neofscryptotest.ECDSAPrivateKey()
	)
	nodeWorkerPool, err := ants.NewPool(1, ants.WithNonblocking(true))
	require.NoError(t, err)

	cluster := newTestClusterForRepPolicy(t, 1, 1, 1)

	cnr := containertest.Container()
	rep := netmap.ReplicaDescriptor{}
	rep.SetNumberOfObjects(1)
	p := netmaptest.PlacementPolicy()
	p.SetReplicas([]netmap.ReplicaDescriptor{rep})
	cnr.SetPlacementPolicy(p)

	s := NewService(cluster.nodeServices, &cluster.nodeNetworks[0], nil,
		quotas{hard: hardLimit},
		&payments{},
		WithLogger(zaptest.NewLogger(t)),
		WithKeyStorage(objutil.NewKeyStorage(&nodeKey, cluster.nodeSessions[0], &cluster.nodeNetworks[0])),
		WithObjectStorage(&cluster.nodeLocalStorages[0]),
		WithMaxSizeSource(mockMaxSize(maxObjectSize)),
		WithContainerSource(mockContainer(cnr)),
		WithNetworkState(&cluster.nodeNetworks[0]),
		WithClientConstructor(cluster.nodeServices),
		WithSplitChainVerifier(mockSplitVerifier{}),
		WithRemoteWorkerPool(nodeWorkerPool),
	)

	cID := cidtest.ID()
	owner := usertest.User()

	var sessionToken session.Object
	sessionToken.SetID(uuid.New())
	sessionToken.SetExp(1)
	sessionToken.BindContainer(cID)
	sessionToken.SetAuthKey(cluster.nodeSessions[0].signer.Public())
	require.NoError(t, sessionToken.Sign(owner))

	req := &protoobject.PutRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			Ttl:          2,
			SessionToken: sessionToken.ProtoMessage(),
		},
	}
	commonPrm, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		panic(err)
	}

	sessionTokenV2 := newSessionTokenV2(t, cID, owner, cluster.nodeSessions, []sessionv2.Verb{sessionv2.VerbObjectPut})

	reqV2 := &protoobject.PutRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			Ttl:            2,
			SessionTokenV2: sessionTokenV2.ProtoMessage(),
		},
	}
	commonPrmV2, err := objutil.CommonPrmFromRequest(reqV2)
	if err != nil {
		panic(err)
	}

	t.Run("known size before streaming", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		o := objecttest.Object()
		o.SetPayloadSize(hardLimit + 1)
		o.SetContainerID(cID)
		o.SetOwner(owner.ID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)

		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrm)

		err = stream.Init(ip)
		require.ErrorIs(t, err, apistatus.QuotaExceeded{})
	})

	t.Run("known size before streaming/sessionv2", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		o := objecttest.Object()
		o.SetPayloadSize(hardLimit + 1)
		o.SetContainerID(cID)
		o.SetOwner(owner.ID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)

		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrmV2)

		err = stream.Init(ip)
		require.ErrorIs(t, err, apistatus.QuotaExceeded{})
	})

	t.Run("payload exceeded", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		o := objecttest.Object()
		o.SetPayloadSize(hardLimit - 1)
		o.SetContainerID(cID)
		o.SetOwner(owner.ID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)

		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrm)
		err = stream.Init(ip)
		require.NoError(t, err)

		sendPrm := PutChunkPrm{make([]byte, hardLimit+1)}
		err = stream.SendChunk(&sendPrm)
		require.ErrorIs(t, err, apistatus.ErrQuotaExceeded)
	})

	t.Run("payload exceeded/sessionv2", func(t *testing.T) {
		stream, err := s.Put(context.Background())
		require.NoError(t, err)

		o := objecttest.Object()
		o.SetPayloadSize(hardLimit - 1)
		o.SetContainerID(cID)
		o.SetOwner(owner.ID)
		o.ResetRelations()
		o.SetType(object.TypeRegular)

		ip := new(PutInitPrm).
			WithObject(&o).
			WithCommonPrm(commonPrmV2)
		err = stream.Init(ip)
		require.NoError(t, err)

		sendPrm := PutChunkPrm{make([]byte, hardLimit+1)}
		err = stream.SendChunk(&sendPrm)
		require.ErrorIs(t, err, apistatus.ErrQuotaExceeded)
	})
}

func Test_Slicing_REP3(t *testing.T) {
	const repNodes = 3
	const cnrReserveNodes = 2
	const outCnrNodes = 2

	cluster := newTestClusterForRepPolicy(t, repNodes, cnrReserveNodes, outCnrNodes)

	for _, tc := range []struct {
		name string
		ln   uint64
	}{
		{name: "no payload", ln: 0},
		{name: "1B", ln: 1},
		{name: "limit-1B", ln: maxObjectSize - 1},
		{name: "exactly limit", ln: maxObjectSize},
		{name: "limit+1b", ln: maxObjectSize + 1},
		{name: "limitX2", ln: maxObjectSize * 2},
		{name: "limitX4-1", ln: maxObjectSize + 4 - 1},
		{name: "limitX5", ln: maxObjectSize * 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testSlicingREP3(t, cluster, tc.ln, repNodes, cnrReserveNodes, outCnrNodes, false)
			t.Run("sessionv2", func(t *testing.T) {
				testSlicingREP3(t, cluster, tc.ln, repNodes, cnrReserveNodes, outCnrNodes, true)
			})
		})
	}

	t.Run("tombstone", func(t *testing.T) {
		testTombstoneSlicing(t, cluster, repNodes+cnrReserveNodes, outCnrNodes, false)
		t.Run("sessionv2", func(t *testing.T) {
			testTombstoneSlicing(t, cluster, repNodes+cnrReserveNodes, outCnrNodes, true)
		})
	})

	t.Run("lock", func(t *testing.T) {
		testLockSlicing(t, cluster, repNodes+cnrReserveNodes, outCnrNodes, false)
		t.Run("sessionv2", func(t *testing.T) {
			testLockSlicing(t, cluster, repNodes+cnrReserveNodes, outCnrNodes, true)
		})
	})
}

func Test_Slicing_EC(t *testing.T) {
	rules := []iec.Rule{
		{DataPartNum: 2, ParityPartNum: 2},
		{DataPartNum: 3, ParityPartNum: 1},
		{DataPartNum: 6, ParityPartNum: 3},
		{DataPartNum: 12, ParityPartNum: 4},
	}

	maxRule := slices.MaxFunc(rules, func(a, b iec.Rule) int {
		return cmp.Compare(a.DataPartNum+a.ParityPartNum, b.DataPartNum+b.ParityPartNum)
	})

	maxTotalParts := int(maxRule.DataPartNum + maxRule.ParityPartNum)
	const cnrReserveNodes = 2
	const outCnrNodes = 2

	var cnr container.Container
	var policy netmap.PlacementPolicy
	ecRules := make([]netmap.ECRule, len(rules))
	for i := range rules {
		ecRules[i].SetDataPartNum(uint32(rules[i].DataPartNum))
		ecRules[i].SetParityPartNum(uint32(rules[i].ParityPartNum))
	}
	policy.SetECRules(ecRules)
	cnr.SetPlacementPolicy(policy)

	cluster := newTestClusterForRepPolicyWithContainer(t, uint(maxTotalParts), cnrReserveNodes, outCnrNodes, cnr)
	for i := range cluster.nodeNetworks {
		// TODO: add alternative to newTestClusterForRepPolicy for EC instead
		cluster.nodeNetworks[i].cnrNodes.repCounts = nil
		for range len(rules) - 1 {
			cluster.nodeNetworks[i].cnrNodes.unsorted = append(cluster.nodeNetworks[i].cnrNodes.unsorted, cluster.nodeNetworks[i].cnrNodes.unsorted[0])
			cluster.nodeNetworks[i].cnrNodes.sorted = append(cluster.nodeNetworks[i].cnrNodes.sorted, cluster.nodeNetworks[i].cnrNodes.sorted[0])
		}
		cluster.nodeNetworks[i].cnrNodes.ecRules = rules
	}

	for _, tc := range []struct {
		name string
		ln   uint64
		skip string
	}{
		{name: "no payload", ln: 0},
		{name: "1B", ln: 1},
		{name: "limit-1B", ln: maxObjectSize - 1},
		{name: "exactly limit", ln: maxObjectSize},
		{name: "limit+1b", ln: maxObjectSize + 1, skip: "https://github.com/nspcc-dev/neofs-node/issues/3500"},
		{name: "limitX2", ln: maxObjectSize * 2, skip: "https://github.com/nspcc-dev/neofs-node/issues/3500"},
		{name: "limitX4-1", ln: maxObjectSize + 4 - 1, skip: "https://github.com/nspcc-dev/neofs-node/issues/3500"},
		{name: "limitX5", ln: maxObjectSize * 5, skip: "https://github.com/nspcc-dev/neofs-node/issues/3500"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			testSlicingECRules(t, cluster, tc.ln, rules, maxTotalParts, cnrReserveNodes, outCnrNodes, false)
			t.Run("sessionv2", func(t *testing.T) {
				testSlicingECRules(t, cluster, tc.ln, rules, maxTotalParts, cnrReserveNodes, outCnrNodes, true)
			})
		})
	}

	t.Run("tombstone", func(t *testing.T) {
		testTombstoneSlicing(t, cluster, maxTotalParts+cnrReserveNodes, outCnrNodes, false)
		t.Run("sessionv2", func(t *testing.T) {
			testTombstoneSlicing(t, cluster, maxTotalParts+cnrReserveNodes, outCnrNodes, true)
		})
	})

	t.Run("lock", func(t *testing.T) {
		testLockSlicing(t, cluster, maxTotalParts+cnrReserveNodes, outCnrNodes, false)
		t.Run("sessionv2", func(t *testing.T) {
			testLockSlicing(t, cluster, maxTotalParts+cnrReserveNodes, outCnrNodes, true)
		})
	})
}

func testSlicingREP3(t *testing.T, cluster *testCluster, ln uint64, repNodes, cnrReserveNodes, outCnrNodes int, isSessionTokenV2 bool) {
	owner := usertest.User()

	var srcObj object.Object
	srcObj.SetContainerID(cidtest.ID())
	srcObj.SetOwner(owner.UserID())
	srcObj.SetAttributes(
		object.NewAttribute("attr1", "val1"),
		object.NewAttribute("attr2", "val2"),
	)
	srcObj.SetPayload(testutil.RandByteSlice(ln))

	var (
		sessionToken   *session.Object
		sessionTokenV2 *sessionv2.Token
	)
	if isSessionTokenV2 {
		sessionTokenV2 = newSessionTokenV2(t, cidtest.ID(), owner, cluster.nodeSessions, []sessionv2.Verb{sessionv2.VerbObjectPut})
	} else {
		sessionToken = &session.Object{}
		sessionToken.SetID(uuid.New())
		sessionToken.SetExp(1)
		sessionToken.BindContainer(cidtest.ID())
	}

	testThroughNode := func(t *testing.T, idx int) {
		if isSessionTokenV2 {
			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, nil, sessionTokenV2)
		} else {
			sessionToken.SetAuthKey(cluster.nodeSessions[idx].signer.Public())
			require.NoError(t, sessionToken.Sign(owner))

			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, sessionToken, nil)
		}

		nodeObjLists := cluster.allStoredObjects()

		var restoredObj object.Object
		if ln > maxObjectSize {
			restoredObj = assertSplitChain(t, maxObjectSize, ln, sessionToken, sessionTokenV2, nodeObjLists[0])

			for i := 1; i < repNodes; i++ {
				require.Equal(t, nodeObjLists[0], nodeObjLists[i], i)
			}
			linkerOnly := []object.Object{nodeObjLists[0][len(nodeObjLists[0])-1]}
			for i := repNodes; i < repNodes+cnrReserveNodes; i++ {
				require.Equal(t, linkerOnly, nodeObjLists[i], i)
			}
			for i := repNodes + cnrReserveNodes; i < len(nodeObjLists); i++ {
				require.Empty(t, nodeObjLists[i], i)
			}
		} else {
			require.Len(t, nodeObjLists[0], 1)
			restoredObj = nodeObjLists[0][0]

			for i := 1; i < repNodes; i++ {
				require.Len(t, nodeObjLists[i], 1, i)

				obj := nodeObjLists[i][0]
				// require.Equal(t, restoredObj, obj) can fail for empty payload because []byte{} != []byte(nil)
				require.Equal(t, restoredObj.CutPayload(), obj.CutPayload())
				require.True(t, bytes.Equal(restoredObj.Payload(), obj.Payload()))
			}
			for i := repNodes; i < len(nodeObjLists); i++ {
				require.Empty(t, nodeObjLists[i], i)
			}
		}

		assertObjectIntegrity(t, restoredObj)
		require.True(t, bytes.Equal(srcObj.Payload(), restoredObj.Payload()))
		require.Equal(t, srcObj.GetContainerID(), restoredObj.GetContainerID())
		if isSessionTokenV2 {
			require.Equal(t, sessionTokenV2, restoredObj.SessionTokenV2())
			require.Equal(t, sessionTokenV2.Issuer(), restoredObj.Owner())
		} else {
			require.Equal(t, sessionToken, restoredObj.SessionToken())
			require.Equal(t, sessionToken.Issuer(), restoredObj.Owner())
		}
		require.EqualValues(t, currentEpoch, restoredObj.CreationEpoch())
		require.Equal(t, object.TypeRegular, restoredObj.Type())
		require.Equal(t, srcObj.Attributes(), restoredObj.Attributes())
		require.False(t, restoredObj.HasParent())

		cluster.resetAllStoredObjects()
	}

	for i := range repNodes + cnrReserveNodes + outCnrNodes {
		testThroughNode(t, i)
	}
}

func testSlicingECRules(t *testing.T, cluster *testCluster, ln uint64, rules []iec.Rule, maxTotalParts, cnrReserveNodes, outCnrNodes int, isSessionV2 bool) {
	owner := usertest.User()
	var srcObj object.Object
	srcObj.SetContainerID(cidtest.ID())
	srcObj.SetOwner(owner.UserID())
	srcObj.SetAttributes(
		object.NewAttribute("attr1", "val1"),
		object.NewAttribute("attr2", "val2"),
	)

	var (
		sessionTokenV2 *sessionv2.Token
		sessionToken   *session.Object
	)

	if isSessionV2 {
		sessionTokenV2 = newSessionTokenV2(t, cidtest.ID(), owner, nil, []sessionv2.Verb{sessionv2.VerbObjectPut})
	} else {
		sessionToken = &session.Object{}
		sessionToken.SetID(uuid.New())
		sessionToken.SetExp(1)
		sessionToken.BindContainer(cidtest.ID())
		srcObj.SetPayload(testutil.RandByteSlice(ln))
	}

	testThroughNode := func(t *testing.T, idx int) {
		if isSessionV2 {
			pk := cluster.nodeSessions[idx].signer.ECDSAPrivateKey.PublicKey
			require.NoError(t, sessionTokenV2.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(user.NewFromECDSAPublicKey(pk))}))

			require.NoError(t, sessionTokenV2.Sign(owner))
			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, nil, sessionTokenV2)
		} else {
			sessionToken.SetAuthKey(cluster.nodeSessions[idx].signer.Public())
			require.NoError(t, sessionToken.Sign(owner))

			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, sessionToken, nil)
		}

		nodeObjLists := cluster.allStoredObjects()

		var restoredObj object.Object
		if ln > maxObjectSize {
			restoredObj = checkAndCutSplitECObject(t, ln, sessionToken, sessionTokenV2, rules, nodeObjLists)
		} else {
			restoredObj = checkAndCutUnsplitECObject(t, rules, nodeObjLists)
		}

		require.Zero(t, islices.TwoDimSliceElementCount(nodeObjLists))

		assertObjectIntegrity(t, restoredObj)
		require.Equal(t, srcObj.GetContainerID(), restoredObj.GetContainerID())
		if isSessionV2 {
			require.Equal(t, sessionTokenV2, restoredObj.SessionTokenV2())
			require.Equal(t, sessionTokenV2.Issuer(), restoredObj.Owner())
		} else {
			require.Equal(t, sessionToken, restoredObj.SessionToken())
			require.Equal(t, sessionToken.Issuer(), restoredObj.Owner())
		}
		require.EqualValues(t, currentEpoch, restoredObj.CreationEpoch())
		require.Equal(t, object.TypeRegular, restoredObj.Type())
		require.Equal(t, srcObj.Attributes(), restoredObj.Attributes())
		require.False(t, restoredObj.HasParent())
		require.True(t, bytes.Equal(srcObj.Payload(), restoredObj.Payload()))

		cluster.resetAllStoredObjects()
	}

	for i := range maxTotalParts + cnrReserveNodes + outCnrNodes {
		testThroughNode(t, i)
	}
}

func testTombstoneSlicing(t *testing.T, cluster *testCluster, cnrNodeNum, outCnrNodeNum int, isSessionV2 bool) {
	testSysObjectSlicing(t, cluster, cnrNodeNum, outCnrNodeNum, object.TypeTombstone, (*object.Object).AssociateDeleted, isSessionV2)
}

func testLockSlicing(t *testing.T, cluster *testCluster, cnrNodeNum, outCnrNodeNum int, isSessionV2 bool) {
	testSysObjectSlicing(t, cluster, cnrNodeNum, outCnrNodeNum, object.TypeLock, (*object.Object).AssociateLocked, isSessionV2)
}

func testSysObjectSlicing(t *testing.T, cluster *testCluster, cnrNodeNum, outCnrNodeNum int, typ object.Type, associate func(*object.Object, oid.ID), isSessionV2 bool) {
	target := oidtest.ID()
	owner := usertest.User()

	var verCur = version.Current()
	var srcObj object.Object
	srcObj.SetVersion(&verCur)
	srcObj.SetContainerID(cidtest.ID())
	srcObj.SetOwner(owner.UserID())
	srcObj.SetAttributes(
		object.NewAttribute(object.AttributeExpirationEpoch, "123"),
	)
	associate(&srcObj, target)

	var (
		sessionToken   *session.Object
		sessionTokenV2 *sessionv2.Token
	)
	if isSessionV2 {
		sessionTokenV2 = newSessionTokenV2(t, cidtest.ID(), owner, nil, []sessionv2.Verb{sessionv2.VerbObjectPut})
	} else {
		sessionToken = &session.Object{}
		sessionToken.SetID(uuid.New())
		sessionToken.SetExp(1)
		sessionToken.BindContainer(cidtest.ID())
	}

	testThroughNode := func(t *testing.T, idx int) {
		if isSessionV2 {
			pk := cluster.nodeSessions[idx].signer.ECDSAPrivateKey.PublicKey
			require.NoError(t, sessionTokenV2.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(user.NewFromECDSAPublicKey(pk))}))

			require.NoError(t, sessionTokenV2.Sign(owner))

			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, nil, sessionTokenV2)
		} else {
			sessionToken.SetAuthKey(cluster.nodeSessions[idx].signer.Public())
			require.NoError(t, sessionToken.Sign(usertest.User()))

			storeObjectWithSession(t, cluster.nodeServices[idx], srcObj, sessionToken, nil)
		}

		nodeObjLists := cluster.allStoredObjects()

		var restoredObj object.Object
		for i := range cnrNodeNum { // tombstones are broadcast
			require.Len(t, nodeObjLists[i], 1)

			obj := nodeObjLists[i][0]

			if i == 0 {
				restoredObj = obj
			} else {
				require.Equal(t, restoredObj.Marshal(), obj.Marshal())
			}
		}

		require.Zero(t, islices.TwoDimSliceElementCount(nodeObjLists[cnrNodeNum:]))

		assertObjectIntegrity(t, restoredObj)
		require.Empty(t, restoredObj.Payload())
		require.Equal(t, srcObj.GetContainerID(), restoredObj.GetContainerID())
		if isSessionV2 {
			require.Equal(t, sessionTokenV2, restoredObj.SessionTokenV2())
			require.Equal(t, sessionTokenV2.Issuer(), restoredObj.Owner())
		} else {
			require.Equal(t, sessionToken, restoredObj.SessionToken())
			require.Equal(t, sessionToken.Issuer(), restoredObj.Owner())
		}
		require.EqualValues(t, currentEpoch, restoredObj.CreationEpoch())
		require.Equal(t, typ, restoredObj.Type())
		require.Equal(t, target, restoredObj.AssociatedObject())
		require.Equal(t, srcObj.Attributes(), restoredObj.Attributes())
		require.False(t, restoredObj.HasParent())

		cluster.resetAllStoredObjects()
	}

	for i := range cnrNodeNum + outCnrNodeNum {
		testThroughNode(t, i)
	}

	t.Run("all nodes failed", func(t *testing.T) {
		for i := range cnrNodeNum {
			cluster.nodeLocalStorages[i].err = errors.New("some error")
		}

		for i := range cnrNodeNum {
			var err error
			if isSessionV2 {
				pk := cluster.nodeSessions[i].signer.ECDSAPrivateKey.PublicKey
				require.NoError(t, sessionTokenV2.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(user.NewFromECDSAPublicKey(pk))}))

				require.NoError(t, sessionTokenV2.Sign(owner))
				err = putObjectWithSession(cluster.nodeServices[i], srcObj, nil, sessionTokenV2)
			} else {
				sessionToken.SetAuthKey(cluster.nodeSessions[i].signer.Public())
				require.NoError(t, sessionToken.Sign(owner))

				err = putObjectWithSession(cluster.nodeServices[i], srcObj, sessionToken, nil)
			}
			require.ErrorContains(t, err, "incomplete object PUT by placement: number of replicas cannot be met for list #0")
			require.ErrorContains(t, err, "some error")
			require.NotErrorIs(t, err, apistatus.ErrIncomplete)
		}

		for i := range cnrNodeNum {
			cluster.nodeLocalStorages[i].err = nil
		}
	})
}

func newTestClusterForRepPolicy(t *testing.T, repNodes, cnrReserveNodes, outCnrNodes uint) *testCluster {
	return newTestClusterForRepPolicyWithContainer(t, repNodes, cnrReserveNodes, outCnrNodes, container.Container{})
}

func newTestClusterForRepPolicyWithContainer(t *testing.T, repNodes, cnrReserveNodes, outCnrNodes uint, cnr container.Container) *testCluster {
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
		nodeKey, err := keys.NewPrivateKey()
		require.NoError(t, err)
		allNodes[i].SetPublicKey(nodeKey.PublicKey().Bytes())

		nodeWorkerPool, err := ants.NewPool(len(cnrNodes), ants.WithNonblocking(true))
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
			expiresAt: cluster.nodeNetworks[i].epoch + 1,
		}

		cluster.nodeServices[i] = NewService(cluster.nodeServices, &cluster.nodeNetworks[i], nil,
			quotas{math.MaxUint64, math.MaxUint64},
			&payments{},
			WithLogger(zaptest.NewLogger(t).With(zap.Int("node", i))),
			WithKeyStorage(objutil.NewKeyStorage(&nodeKey.PrivateKey, cluster.nodeSessions[i], &cluster.nodeNetworks[i])),
			WithObjectStorage(&cluster.nodeLocalStorages[i]),
			WithMaxSizeSource(mockMaxSize(maxObjectSize)),
			WithContainerSource(mockContainer(cnr)),
			WithNetworkState(&cluster.nodeNetworks[i]),
			WithClientConstructor(cluster.nodeServices),
			WithSplitChainVerifier(mockSplitVerifier{}),
			WithRemoteWorkerPool(nodeWorkerPool),
			WithTombstoneVerifier(mockTombstoneVerifier{}),
		)
	}

	return &cluster
}

type mockSplitVerifier struct{}

func (mockSplitVerifier) VerifySplit(context.Context, cid.ID, oid.ID, []object.MeasuredObject) error {
	return nil
}

type mockTombstoneVerifier struct {
}

func (mockTombstoneVerifier) VerifyTomb(context.Context, cid.ID, object.Tombstone) error {
	panic("unimplemented")
}

func (mockTombstoneVerifier) VerifyTombStoneWithoutPayload(context.Context, object.Object) error {
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

func (*mockNetwork) GetEpochBlockByTime(uint32) (uint32, error) {
	panic("unimplemented")
}

type mockContainerNodes struct {
	unsorted  [][]netmap.NodeInfo
	sorted    [][]netmap.NodeInfo
	repCounts []uint
	ecRules   []iec.Rule
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

func (x mockContainerNodes) ECRules() []iec.Rule {
	return x.ecRules
}

type mockMaxSize uint64

func (x mockMaxSize) MaxObjectSize() uint64 {
	return uint64(x)
}

type mockNodeSession struct {
	signer    neofscryptotest.VariableSigner
	expiresAt uint64
}

func (x mockNodeSession) FindTokenBySubjects(user.ID, []sessionv2.Target) *storage.PrivateToken {
	return storage.NewPrivateToken(&x.signer.ECDSAPrivateKey, x.expiresAt)
}

func (x mockNodeSession) GetToken(user.ID, []byte) *storage.PrivateToken {
	return storage.NewPrivateToken(&x.signer.ECDSAPrivateKey, x.expiresAt)
}

type mockNodeState struct {
	epoch uint64
}

func (x mockNodeState) CurrentEpoch() uint64 {
	return x.epoch
}

type inMemLocalStorage struct {
	unimplementedLocalStorage
	mtx  sync.RWMutex
	objs []object.Object
	err  error
}

func (x *inMemLocalStorage) Put(obj *object.Object, objBin []byte) error {
	if x.err != nil {
		return x.err
	}

	if objBin != nil {
		got := obj.Marshal()
		if len(obj.Payload()) == 0 && len(objBin) == len(got)+2 {
			// this may happen because encodeReplicateRequestWithoutPayload, unlike Marshal,
			// adds field tag for payload even when it is empty
			emptyPayloadField := protowire.AppendTag(nil, 4, protowire.BytesType)
			emptyPayloadField = protowire.AppendBytes(emptyPayloadField, nil)
			if bytes.Equal(objBin[len(got):], emptyPayloadField) {
				objBin = objBin[:len(got)]
			}
		}
		if !bytes.Equal(objBin, got) {
			return errors.New("binary mismatches object")
		}
	}

	var cp object.Object
	obj.CopyTo(&cp)

	x.mtx.Lock()
	x.objs = append(x.objs, cp)
	x.mtx.Unlock()

	return nil
}

type unimplementedLocalStorage struct{}

func (unimplementedLocalStorage) Put(*object.Object, []byte) error {
	panic("unimplemented")
}

func (unimplementedLocalStorage) Delete(oid.Address, uint64, []oid.ID) error {
	panic("unimplemented")
}

func (unimplementedLocalStorage) Lock(oid.Address, []oid.ID) error {
	panic("unimplemented")
}

func (unimplementedLocalStorage) IsLocked(oid.Address) (bool, error) {
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
	return &serviceClient{svc: svc}, nil
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

	if err := svc.ValidateAndStoreObjectLocally(obj); err != nil { //nolint:contextcheck
		return nil, fmt.Errorf("validate and store object locally: %w", err)
	}

	return nil, nil
}

type serviceClient struct {
	unimplementedClient
	svc *Service
}

func (m *serviceClient) ObjectPutInit(ctx context.Context, hdr object.Object, _ user.Signer, _ client.PrmObjectPutInit) (client.ObjectWriter, error) {
	stream, err := m.svc.Put(ctx)
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

type unimplementedClient struct{}

func (unimplementedClient) ObjectPutInit(context.Context, object.Object, user.Signer, client.PrmObjectPutInit) (client.ObjectWriter, error) {
	panic("unimplemented")
}

func (unimplementedClient) ReplicateObject(context.Context, oid.ID, io.ReadSeeker, neofscrypto.Signer, bool) (*neofscrypto.Signature, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectDelete(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectDelete) (oid.ID, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectGetInit(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectGet) (object.Object, *client.PayloadReader, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectHead(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectHead) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectSearchInit(context.Context, cid.ID, user.Signer, client.PrmObjectSearch) (*client.ObjectListReader, error) {
	panic("unimplemented")
}

func (unimplementedClient) SearchObjects(context.Context, cid.ID, object.SearchFilters, []string, string, neofscrypto.Signer, client.SearchObjectsOptions) ([]client.SearchResultItem, string, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectRangeInit(context.Context, cid.ID, oid.ID, uint64, uint64, user.Signer, client.PrmObjectRange) (*client.ObjectRangeReader, error) {
	panic("unimplemented")
}

func (unimplementedClient) ObjectHash(context.Context, cid.ID, oid.ID, user.Signer, client.PrmObjectHash) ([][]byte, error) {
	panic("unimplemented")
}

func (unimplementedClient) AnnounceLocalTrust(context.Context, uint64, []apireputation.Trust, client.PrmAnnounceLocalTrust) error {
	// TODO: interfaces are oversaturated. This will never be needed to server object PUT. Refactor this.
	panic("unimplemented")
}

func (unimplementedClient) AnnounceIntermediateTrust(context.Context, uint64, apireputation.PeerToPeerTrust, client.PrmAnnounceIntermediateTrust) error {
	panic("unimplemented")
}

func (unimplementedClient) ForEachGRPCConn(context.Context, func(context.Context, *grpc.ClientConn) error) error {
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

func (x testCluster) allStoredObjects() [][]object.Object {
	var res [][]object.Object
	for i := range x.nodeLocalStorages {
		res = append(res, x.nodeLocalStorages[i].objs)
	}
	return res
}

func (x *testCluster) resetAllStoredObjects() {
	for i := range x.nodeLocalStorages {
		x.nodeLocalStorages[i].objs = nil
	}
}

func storeObjectWithSession(t *testing.T, svc *Service, obj object.Object, st *session.Object, st2 *sessionv2.Token) {
	require.NoError(t, putObjectWithSession(svc, obj, st, st2))
}

func putObjectWithSession(svc *Service, obj object.Object, st *session.Object, st2 *sessionv2.Token) error {
	stream, err := svc.Put(context.Background())
	if err != nil {
		return fmt.Errorf("init stream: %w", err)
	}

	req := &protoobject.PutRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			Ttl: 2,
		},
	}
	if st2 != nil {
		req.MetaHeader.SessionTokenV2 = st2.ProtoMessage()
	} else if st != nil {
		req.MetaHeader.SessionToken = st.ProtoMessage()
	}

	commonPrm, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return fmt.Errorf("prepare common parameters: %w", err)
	}

	ip := new(PutInitPrm).
		WithObject(obj.CutPayload()).
		WithCommonPrm(commonPrm)
	if err = stream.Init(ip); err != nil {
		return fmt.Errorf("put header: %w", err)
	}

	cp := new(PutChunkPrm).
		WithChunk(obj.Payload())
	if err = stream.SendChunk(cp); err != nil {
		return fmt.Errorf("put header: %w", err)
	}

	if _, err = stream.Close(); err != nil {
		return fmt.Errorf("close stream: %w", err)
	}

	return nil
}

func assertSplitChain(t *testing.T, limit, ln uint64, sessionToken *session.Object, sessionTokenV2 *sessionv2.Token, members []object.Object) object.Object {
	require.Len(t, members, splitMembersCount(limit, ln))

	// all
	for _, member := range members {
		assertObjectIntegrity(t, member)
		require.LessOrEqual(t, member.PayloadSize(), limit)
		if sessionToken != nil {
			require.Equal(t, sessionToken, member.SessionToken())
			require.Equal(t, sessionToken.Issuer(), member.Owner())
		}
		if sessionTokenV2 != nil {
			require.Equal(t, sessionTokenV2, member.SessionTokenV2())
			require.Equal(t, sessionTokenV2.OriginalIssuer(), member.Owner())
		}
		require.EqualValues(t, currentEpoch, member.CreationEpoch())
		require.Empty(t, member.Attributes())
		require.True(t, member.HasParent())
	}

	// payload chunks
	chunks, linker := members[:len(members)-1], members[len(members)-1]

	var gotPayload []byte
	for _, chunk := range chunks {
		require.Equal(t, object.TypeRegular, chunk.Type())

		gotPayload = append(gotPayload, chunk.Payload()...)
	}
	require.Len(t, gotPayload, int(ln))

	require.Zero(t, chunks[0].GetFirstID())
	require.Zero(t, chunks[0].GetPreviousID())
	for i := 1; i < len(chunks); i++ {
		require.Equal(t, chunks[0].GetID(), chunks[i].GetFirstID())
		require.Equal(t, chunks[i-1].GetID(), chunks[i].GetPreviousID())
	}

	// linker
	require.Equal(t, object.TypeLink, linker.Type())
	require.Equal(t, chunks[0].GetID(), linker.GetFirstID())
	require.Zero(t, linker.GetPreviousID())

	var link object.Link
	require.NoError(t, link.Unmarshal(linker.Payload()))
	linkItems := link.Objects()
	require.Len(t, linkItems, len(chunks))
	for i := range linkItems {
		require.Equal(t, chunks[i].GetID(), linkItems[i].ObjectID())
		require.EqualValues(t, chunks[i].PayloadSize(), linkItems[i].ObjectSize())
	}

	// parent
	firstParent := chunks[0].Parent()
	require.NotNil(t, firstParent)
	require.Zero(t, firstParent.PayloadSize())
	_, ok := firstParent.PayloadChecksum()
	require.False(t, ok)
	_, ok = firstParent.PayloadHomomorphicHash()
	require.False(t, ok)
	require.Zero(t, firstParent.GetID())
	require.Zero(t, firstParent.Signature())

	lastParent := chunks[len(chunks)-1].Parent()
	require.NotNil(t, lastParent)
	require.Equal(t, lastParent, linker.Parent())

	for i := 1; i < len(chunks)-1; i++ {
		require.Nil(t, chunks[i].Parent())
	}

	var firstParentCp object.Object
	firstParent.CopyTo(&firstParentCp)
	firstParentCp.SetPayloadSize(lastParent.PayloadSize())
	if cs, ok := lastParent.PayloadChecksum(); ok {
		firstParentCp.SetPayloadChecksum(cs)
	}
	if cs, ok := lastParent.PayloadHomomorphicHash(); ok {
		firstParentCp.SetPayloadHomomorphicHash(cs)
	}
	firstParentCp.SetID(lastParent.GetID())
	firstParentCp.SetSignature(lastParent.Signature())
	require.Equal(t, firstParentCp, *lastParent)

	restored := *lastParent
	restored.SetPayload(gotPayload)

	return restored
}

func splitMembersCount(limit, ln uint64) int {
	if ln <= limit {
		return 1
	}
	res := ln/limit + 1 // + LINK
	if ln%limit != 0 {
		res++
	}
	return int(res)
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

	require.Zero(t, obj.Children())

	require.Zero(t, obj.SplitID())
}

func checkAndGetObjectFromECParts(t *testing.T, limit uint64, rule iec.Rule, parts []object.Object) object.Object {
	require.Len(t, parts, int(rule.DataPartNum+rule.ParityPartNum))

	for _, part := range parts {
		assertObjectIntegrity(t, part)
		require.Zero(t, part.SessionToken())
		require.LessOrEqual(t, part.PayloadSize(), limit)
	}

	hdr := checkAndCutParentHeaderFromECPart(t, parts[0])

	for i := 1; i < len(parts); i++ {
		hdrI := checkAndCutParentHeaderFromECPart(t, parts[i])
		require.Equal(t, hdr, hdrI)
	}

	payload := checkAndGetPayloadFromECParts(t, hdr.PayloadSize(), rule, parts)

	res := hdr
	res.SetPayload(payload)

	return res
}

func checkAndGetPayloadFromECParts(t *testing.T, ln uint64, rule iec.Rule, parts []object.Object) []byte {
	var payloadParts [][]byte
	for i := range parts {
		payloadParts = append(payloadParts, parts[i].Payload())
	}

	if ln == 0 {
		require.Negative(t, slices.IndexFunc(payloadParts, func(e []byte) bool { return len(e) > 0 }))
		return nil
	}

	enc, err := reedsolomon.New(int(rule.DataPartNum), int(rule.ParityPartNum))
	require.NoError(t, err)

	ok, err := enc.Verify(payloadParts)
	require.NoError(t, err)
	require.True(t, ok)

	required := make([]bool, rule.DataPartNum+rule.ParityPartNum)
	for i := range rule.DataPartNum {
		required[i] = true
	}

	for lostCount := 1; lostCount <= int(rule.ParityPartNum); lostCount++ {
		for _, lostIdxs := range islices.IndexCombos(len(payloadParts), lostCount) {
			brokenParts := islices.NilTwoDimSliceElements(payloadParts, lostIdxs)
			require.NoError(t, enc.Reconstruct(brokenParts))
			require.Equal(t, payloadParts, brokenParts)

			brokenParts = islices.NilTwoDimSliceElements(payloadParts, lostIdxs)
			require.NoError(t, enc.ReconstructSome(brokenParts, required))
			require.Equal(t, payloadParts[:rule.DataPartNum], brokenParts[:rule.DataPartNum])
		}
	}

	for _, lostIdxs := range islices.IndexCombos(len(payloadParts), int(rule.ParityPartNum)+1) {
		require.Error(t, enc.Reconstruct(islices.NilTwoDimSliceElements(payloadParts, lostIdxs)))
		require.Error(t, enc.ReconstructSome(islices.NilTwoDimSliceElements(payloadParts, lostIdxs), required))
	}

	payload := slices.Concat(payloadParts[:rule.DataPartNum]...)

	require.GreaterOrEqual(t, uint64(len(payload)), ln)

	require.True(t, islices.AllZeros(payload[ln:]))

	return payload[:ln]
}

func checkAndCutParentHeaderFromECPart(t *testing.T, part object.Object) object.Object {
	par := part.Parent()
	require.NotNil(t, par)

	require.Equal(t, par.Version(), part.Version())
	require.Equal(t, par.GetContainerID(), part.GetContainerID())
	require.Equal(t, par.Owner(), part.Owner())
	require.Equal(t, par.CreationEpoch(), part.CreationEpoch())
	require.Equal(t, object.TypeRegular, part.Type())
	require.True(t, par.SessionToken() != nil || par.SessionTokenV2() != nil)

	return *par
}

func checkAndGetECPartInfo(t testing.TB, part object.Object) (int, int) {
	ruleIdxAttr := iobject.GetAttribute(part, "__NEOFS__EC_RULE_IDX")
	require.NotZero(t, ruleIdxAttr)
	ruleIdx, err := strconv.Atoi(ruleIdxAttr)
	require.NoError(t, err)
	require.True(t, ruleIdx >= 0)

	partIdxAttr := iobject.GetAttribute(part, "__NEOFS__EC_PART_IDX")
	require.NotZero(t, partIdxAttr)
	partIdx, err := strconv.Atoi(partIdxAttr)
	require.NoError(t, err)
	require.True(t, partIdx >= 0)

	return ruleIdx, partIdx
}

func checkAndCutSplitECObject(t *testing.T, ln uint64, sessionToken *session.Object, sessionTokenV2 *sessionv2.Token, rules []iec.Rule, nodeObjLists [][]object.Object) object.Object {
	splitPartCount := splitMembersCount(maxObjectSize, ln)

	var expectedCount int
	for i := range rules {
		expectedCount += int(rules[i].DataPartNum+rules[i].ParityPartNum) * splitPartCount
	}

	require.EqualValues(t, expectedCount, islices.TwoDimSliceElementCount(nodeObjLists))

	var splitParts []object.Object
	for range splitPartCount {
		splitPart := checkAndCutUnsplitECObject(t, rules, nodeObjLists)
		splitParts = append(splitParts, splitPart)
	}

	restoredObj := assertSplitChain(t, maxObjectSize, ln, sessionToken, sessionTokenV2, splitParts)

	return restoredObj
}

func checkAndCutUnsplitECObject(t *testing.T, rules []iec.Rule, nodeObjLists [][]object.Object) object.Object {
	ecParts := checkAndCutECPartsForRule(t, 0, rules[0], nodeObjLists)
	restoredObj := checkAndGetObjectFromECParts(t, maxObjectSize, rules[0], ecParts)

	for i := 1; i < len(rules); i++ {
		ecPartsI := checkAndCutECPartsForRule(t, i, rules[i], nodeObjLists)
		restoredObjI := checkAndGetObjectFromECParts(t, maxObjectSize, rules[i], ecPartsI)
		require.Equal(t, restoredObj, restoredObjI)
	}

	return restoredObj
}

func checkAndCutECPartsForRule(t *testing.T, ruleIdx int, rule iec.Rule, nodeObjLists [][]object.Object) []object.Object {
	var parts []object.Object

	for i := range rule.DataPartNum + rule.ParityPartNum {
		gotRuleIdx, partIdx := checkAndGetECPartInfo(t, nodeObjLists[i][0])
		require.EqualValues(t, ruleIdx, gotRuleIdx)
		require.EqualValues(t, i, partIdx)

		parts = append(parts, nodeObjLists[i][0])
		nodeObjLists[i] = nodeObjLists[i][1:]
	}

	return parts
}

func newSessionTokenV2(t *testing.T, cnrID cid.ID, owner user.Signer, nodes []mockNodeSession, verbs []sessionv2.Verb) *sessionv2.Token {
	var sessionTokenV2 sessionv2.Token
	sessionTokenV2.SetVersion(sessionv2.TokenCurrentVersion)

	currentTime := time.Now()
	sessionTokenV2.SetIat(currentTime)
	sessionTokenV2.SetNbf(currentTime)
	sessionTokenV2.SetExp(currentTime.Add(10 * time.Hour))
	ctx, err := sessionv2.NewContext(cnrID, verbs)
	require.NoError(t, err)
	require.NoError(t, sessionTokenV2.SetContexts([]sessionv2.Context{ctx}))

	if nodes != nil {
		for _, s := range nodes {
			require.NoError(t, sessionTokenV2.AddSubject(sessionv2.NewTargetUser(user.NewFromECDSAPublicKey(s.signer.ECDSAPrivateKey.PublicKey))))
		}

		require.NoError(t, sessionTokenV2.Sign(owner))
	}

	return &sessionTokenV2
}
