package getsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"testing/iotest"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_Get_EC_Part(t *testing.T) {
	ctx := context.Background()
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	parentAddr := oid.NewAddress(cnr, parentID)

	t.Run("invalid request", func(t *testing.T) {
		svc := New(unimplementedNeoFSNet{})
		for _, tc := range []struct {
			name      string
			xs        []string
			assertErr func(t *testing.T, err error)
		}{
			{name: "rule idx only", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX and __NEOFS__EC_PART_IDX X-headers must be set together")
			}},
			{name: "part idx only", xs: []string{
				"__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX and __NEOFS__EC_PART_IDX X-headers must be set together")
			}},
			{name: "empty rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX and __NEOFS__EC_PART_IDX X-headers must be set together")
			}},
			{name: "non-numeric rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "foo", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_RULE_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "float rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0.1", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_RULE_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "negative rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "-1", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_RULE_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "rule idx overflow", xs: []string{
				"__NEOFS__EC_RULE_IDX", "256", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_RULE_IDX X-header")
				require.ErrorContains(t, err, "value out of range")
			}},
			{name: "empty part idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0", "__NEOFS__EC_PART_IDX", "",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX and __NEOFS__EC_PART_IDX X-headers must be set together")
			}},
			{name: "non-numeric part idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0", "__NEOFS__EC_PART_IDX", "foo",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_PART_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "float part idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0", "__NEOFS__EC_PART_IDX", "0.1",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_PART_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "negative part idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0", "__NEOFS__EC_PART_IDX", "-1",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_PART_IDX X-header")
				require.ErrorContains(t, err, "invalid syntax")
			}},
			{name: "part idx overflow", xs: []string{
				"__NEOFS__EC_RULE_IDX", "0", "__NEOFS__EC_PART_IDX", "256",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "invalid __NEOFS__EC_PART_IDX X-header")
				require.ErrorContains(t, err, "value out of range")
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var prm Prm
				prm.WithAddress(parentAddr)
				prm.SetObjectWriter(unimplementedObjectWriter{})
				parameterizeXHeaders(t, &prm, tc.xs) // TODO: parameterizePartInfoString

				err := svc.Get(ctx, prm)
				require.ErrorContains(t, err, "invalid request")
				tc.assertErr(t, err)
			})
		}
	})

	pi := iec.PartInfo{
		RuleIndex: 12,
		Index:     34,
	}

	t.Run("container policy application failure", func(t *testing.T) {
		policyErr := errors.New("some policy error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {err: policyErr},
			},
		})

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(unimplementedObjectWriter{})
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, policyErr)
		require.EqualError(t, err, "get nodes for object: "+policyErr.Error())
	})

	t.Run("non-EC container", func(t *testing.T) {
		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {},
			},
		})

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(unimplementedObjectWriter{})
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.EqualError(t, err, "invalid request: EC part requested in container without EC policy")
	})

	t.Run("local storage failure", func(t *testing.T) {
		localStorageErr := errors.New("some local storage error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {ecRules: []iec.Rule{{}}},
			},
		})
		svc.localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {err: localStorageErr},
			},
		}

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(unimplementedObjectWriter{})
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, localStorageErr)
		require.EqualError(t, err, "get object from local storage: "+localStorageErr.Error())
	})

	var parentHdr object.Object
	parentHdr.SetID(parentID)
	parentHdr.SetContainerID(cnr)
	parentHdr.SetAttributes(
		object.NewAttribute("parent_attr1", "parent_val1"),
		object.NewAttribute("parent_attr2", "parent_val2"),
	)

	partData := testutil.RandByteSlice(32)

	partObj, err := iec.FormObjectForECPart(neofscryptotest.Signer(), parentHdr, partData, pi)
	require.NoError(t, err)
	partHdr := *partObj.CutPayload()

	t.Run("payload reader failure", func(t *testing.T) {
		okData := testutil.RandByteSlice(32)
		readErr := errors.New("some read error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {ecRules: []iec.Rule{{}}},
			},
		})

		var closer mockIOCloser

		svc.localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {
					hdr: partHdr,
					rdr: ioReadCloser{
						Reader: io.MultiReader(bytes.NewReader(okData), iotest.ErrReader(readErr)),
						Closer: &closer,
					},
				},
			},
		}

		var w mockObjectWriter

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(&w)
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, readErr)
		require.EqualError(t, err, "copy object: read next payload chunk: "+readErr.Error())
		require.Equal(t, partHdr, w.hdr)
		require.Equal(t, okData, w.buf.Bytes())

		require.EqualValues(t, 1, closer.count.Load())
	})

	t.Run("respond header failure", func(t *testing.T) {
		writeHeaderErr := errors.New("some write header error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {ecRules: []iec.Rule{{}}},
			},
		})

		var closer mockIOCloser

		svc.localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {
					hdr: partHdr,
					rdr: ioReadCloser{Reader: bytes.NewReader(partData), Closer: &closer},
				},
			},
		}

		w := mockObjectWriter{
			writeHeaderErr: writeHeaderErr,
		}

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(&w)
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, writeHeaderErr)
		require.EqualError(t, err, "copy object: write header: "+writeHeaderErr.Error())
		require.Zero(t, w.buf.Len())

		require.EqualValues(t, 1, closer.count.Load())
	})

	t.Run("respond payload chunk failure", func(t *testing.T) {
		writeChunkErr := errors.New("some write chunk error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {ecRules: []iec.Rule{{}}},
			},
		})

		var closer mockIOCloser

		svc.localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: pi}: {
					hdr: partHdr,
					rdr: ioReadCloser{Reader: bytes.NewReader(partData), Closer: &closer},
				},
			},
		}

		w := mockObjectWriter{
			writeChunkErr: writeChunkErr,
		}

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(&w)
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, writeChunkErr)
		require.EqualError(t, err, "copy object: write next payload chunk: "+writeChunkErr.Error())
		require.Equal(t, partHdr, w.hdr)

		require.EqualValues(t, 1, closer.count.Load())
	})

	svc := New(&mockNeoFSNet{
		getNodesForObject: map[oid.Address]getNodesForObjectValue{
			parentAddr: {ecRules: []iec.Rule{{}}},
		},
	})

	var closer mockIOCloser

	svc.localObjects = &mockLocalObjects{
		getECPart: map[getECPartKey]getECPartValue{
			{cnr: cnr, parent: parentID, pi: pi}: {
				hdr: partHdr,
				rdr: ioReadCloser{Reader: bytes.NewReader(partData), Closer: &closer},
			},
		},
	}

	var w mockObjectWriter

	var prm Prm
	prm.WithAddress(parentAddr)
	prm.SetObjectWriter(&w)
	parameterizePartInfo(t, &prm, pi)

	err = svc.Get(ctx, prm)
	require.NoError(t, err)
	require.Equal(t, partHdr, w.hdr)
	require.Equal(t, partData, w.buf.Bytes())

	require.EqualValues(t, 1, closer.count.Load())
}

func TestService_Get_EC(t *testing.T) {
	ctx := context.Background()
	cnr := cidtest.ID()
	parentID := oidtest.ID()
	parentAddr := oid.NewAddress(cnr, parentID)

	t.Run("container policy application failure", func(t *testing.T) {
		policyErr := errors.New("some policy error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {err: policyErr},
			},
		})

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetObjectWriter(unimplementedObjectWriter{})

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, policyErr)
		require.EqualError(t, err, "get nodes for object: "+policyErr.Error())
	})

	signer := usertest.User()
	nodeKey := neofscryptotest.ECDSAPrivateKey()

	parentPayload := testutil.RandByteSlice(4 << 10)

	var parentHdr object.Object
	parentHdr.SetID(parentID)
	parentHdr.SetContainerID(cnr)
	parentHdr.SetAttributes(
		object.NewAttribute("parent_attr1", "parent_val1"),
		object.NewAttribute("parent_attr2", "parent_val2"),
	)
	parentHdr.SetPayloadSize(uint64(len(parentPayload)))

	rule := iec.Rule{
		DataPartNum:   12,
		ParityPartNum: 4,
	}

	parts, err := iec.Encode(rule, parentPayload)
	require.NoError(t, err)

	partObjs := make([]object.Object, len(parts))
	for i := range parts {
		partObjs[i], err = iec.FormObjectForECPart(signer, parentHdr, parts[i], iec.PartInfo{Index: i})
		require.NoError(t, err)
	}

	sTok := sessiontest.ObjectSigned(signer)

	nodeLists, _ := testNodeMatrix(t, []int{int(rule.DataPartNum + rule.ParityPartNum)})

	getNodesMap := map[oid.Address]getNodesForObjectValue{
		parentAddr: {nodeSets: nodeLists, ecRules: []iec.Rule{rule}},
	}

	nodeSvcs := make([]*Service, rule.DataPartNum+rule.ParityPartNum)
	for i := range nodeSvcs {
		svc := New(&mockNeoFSNet{
			getNodesForObject: getNodesMap,
			localPubKey:       nodeLists[0][i].PublicKey(),
		})
		svc.localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}: {
					hdr: *partObjs[i].CutPayload(),
					rdr: io.NopCloser(bytes.NewReader(partObjs[i].Payload())),
				},
			},
		}

		nodeSvcs[i] = svc
	}

	tc := &testECServiceConn{
		mockKeyStorage: mockKeyStorage{
			privKey: nodeKey,
		},
		sTok:  &sTok,
		nodes: make(map[string]*Service),
	}
	for i := range nodeSvcs {
		tc.nodes[string(nodeLists[0][i].Marshal())] = nodeSvcs[i]
	}

	cp, err := newCommonParameters(false, &sTok, nil)
	require.NoError(t, err)

	var prm Prm
	prm.WithAddress(parentAddr)
	prm.SetCommonParameters(cp)

	newService := func(*testing.T) *Service {
		svc := New(&mockNeoFSNet{
			getNodesForObject: getNodesMap,
		})
		svc.keyStore = tc
		svc.conns = tc

		return svc
	}

	t.Run("already removed", func(t *testing.T) {
		for i := range nodeSvcs {
			localVal := getECPartValue{
				hdr: *partObjs[i].CutPayload(),
				rdr: io.NopCloser(bytes.NewReader(partObjs[i].Payload())),
			}
			if i == int(rule.DataPartNum-1) {
				localVal.err = fmt.Errorf("some context: %w", apistatus.ErrObjectAlreadyRemoved)
			}

			nodeSvcs[i].localObjects = &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}: localVal,
				},
			}
		}

		var w mockObjectWriter
		prm.SetObjectWriter(&w)

		svc := newService(t)

		err = svc.Get(ctx, prm)
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	})

	t.Run("recover from parity", func(t *testing.T) {
		for i := range nodeSvcs {
			key := getECPartKey{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}

			var val getECPartValue
			if i <= int(rule.DataPartNum-rule.ParityPartNum) || i >= int(rule.DataPartNum) {
				val.hdr = *partObjs[i].CutPayload()
				val.rdr = io.NopCloser(bytes.NewReader(partObjs[i].Payload()))
			} else {
				val.err = errors.New("some error")
			}

			nodeSvcs[i].localObjects = &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					key: val,
				},
			}
		}

		var w mockObjectWriter
		prm.SetObjectWriter(&w)

		svc := newService(t)

		err = svc.Get(ctx, prm)
		require.NoError(t, err)
		require.Equal(t, parentHdr, w.hdr)
		require.Equal(t, parentPayload, w.buf.Bytes())
	})

	for i := range nodeSvcs {
		nodeSvcs[i].localObjects = &mockLocalObjects{
			getECPart: map[getECPartKey]getECPartValue{
				{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}: {
					hdr: *partObjs[i].CutPayload(),
					rdr: io.NopCloser(bytes.NewReader(partObjs[i].Payload())),
				},
			},
		}
	}

	var w mockObjectWriter
	prm.SetObjectWriter(&w)

	svc := newService(t)

	err = svc.Get(ctx, prm)
	require.NoError(t, err)
	require.Equal(t, parentHdr, w.hdr)
	require.Equal(t, parentPayload, w.buf.Bytes())
}

func parameterizePartInfo(t testing.TB, p *Prm, pi iec.PartInfo) {
	parameterizePartInfoString(t, p, strconv.Itoa(pi.RuleIndex), strconv.Itoa(pi.Index))
}

func parameterizePartInfoString(t testing.TB, p *Prm, ruleIdx, partIdx string) {
	parameterizeXHeaders(t, p, []string{
		"__NEOFS__EC_RULE_IDX", ruleIdx,
		"__NEOFS__EC_PART_IDX", partIdx,
	})
}

type testECServiceConn struct {
	unimplementedServiceConns
	mockKeyStorage
	sTok *session.Object

	nodes map[string]*Service
}

func (x *testECServiceConn) InitGetObjectStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey,
	cnr cid.ID, id oid.ID, sTok *session.Object, local, verifyID bool, xs []string) (object.Object, io.ReadCloser, error) {
	if ctx == nil {
		return object.Object{}, nil, errors.New("[test] missing context")
	}
	if !local {
		return object.Object{}, nil, errors.New("[test] non-local request")
	}
	if verifyID {
		return object.Object{}, nil, errors.New("[test] request with ID verification")
	}
	if !pk.Equal(&x.privKey) {
		return object.Object{}, nil, errors.New("[test] unexpected private key")
	}
	if !assert.ObjectsAreEqual(sTok, x.sTok) {
		return object.Object{}, nil, errors.New("[test] unexpected session token")
	}

	v, ok := x.nodes[string(node.Marshal())]
	if !ok {
		return object.Object{}, nil, errors.New("[test] unexpected node")
	}

	cp, err := newCommonParameters(local, sTok, xs)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("newCommonParameters: %w", err)
	}

	var w mockObjectWriter

	var prm Prm
	prm.WithAddress(oid.NewAddress(cnr, id))
	prm.SetObjectWriter(&w)
	prm.SetCommonParameters(cp)

	if err := v.Get(ctx, prm); err != nil {
		return object.Object{}, nil, err
	}

	return w.hdr, io.NopCloser(&w.buf), nil
}
