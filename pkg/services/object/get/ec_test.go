package getsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
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
			{name: "part idx only", xs: []string{
				"__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX X-header must be set in a correct EC part GET request")
			}},
			{name: "empty rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "__NEOFS__EC_RULE_IDX X-header must be set in a correct EC part GET request")
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

	ecRules := make([]iec.Rule, pi.RuleIndex+1)
	ecRules[pi.RuleIndex].DataPartNum = 30
	ecRules[pi.RuleIndex].ParityPartNum = 5

	t.Run("non-EC container", func(t *testing.T) {
		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {},
			},
		})

		var cnr container.Container

		cnr.SetBasicACL(0xff) // Fake value.

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.WithContainer(cnr)
		prm.SetObjectWriter(unimplementedObjectWriter{})
		parameterizePartInfo(t, &prm, pi)

		err := svc.Get(ctx, prm)
		require.EqualError(t, err, "invalid request: EC part requested in container without EC policy")
	})

	t.Run("local storage failure", func(t *testing.T) {
		localStorageErr := errors.New("some local storage error")

		svc := New(&mockNeoFSNet{
			getNodesForObject: map[oid.Address]getNodesForObjectValue{
				parentAddr: {ecRules: ecRules},
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
				parentAddr: {ecRules: ecRules},
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
				parentAddr: {ecRules: ecRules},
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
				parentAddr: {ecRules: ecRules},
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
		require.EqualError(t, err, "copy object: stream failure: "+writeChunkErr.Error())
		require.Equal(t, partHdr, w.hdr)

		require.EqualValues(t, 1, closer.count.Load())
	})

	t.Run("placement build failure", func(t *testing.T) {
		placementErr := errors.New("some placement error")
		svc := New(&mockNeoFSNet{err: placementErr})

		var prm Prm
		prm.WithAddress(parentAddr)
		parameterizePartInfoString(t, &prm, "1", "")

		err := svc.Get(ctx, prm)
		require.ErrorIs(t, err, placementErr)
		require.EqualError(t, err, fmt.Sprintf("invalid request: missing %s and calculating node's EC part finished with error: %s", iec.AttributePartIdx, placementErr))
	})

	svc := New(&mockNeoFSNet{
		getNodesForObject: map[oid.Address]getNodesForObjectValue{
			parentAddr: {ecRules: ecRules},
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

	parts, _, err := iec.Encode(rule, parentPayload)
	require.NoError(t, err)

	partObjs := make([]object.Object, len(parts))
	for i := range parts {
		partObjs[i], err = iec.FormObjectForECPart(signer, parentHdr, parts[i], iec.PartInfo{Index: i})
		require.NoError(t, err)
	}

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
		parentHdr: parentHdr,
		nodes:     make(map[string]*Service),
	}
	for i := range nodeSvcs {
		tc.nodes[string(nodeLists[0][i].Marshal())] = nodeSvcs[i]
	}

	cp := newCommonParameters(false, nil)

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
					pld: partObjs[i].Payload(),
					rdr: io.NopCloser(bytes.NewReader(partObjs[i].Payload())),
				},
			},
		}
	}

	t.Run("payload-only ranged GET validates header", func(t *testing.T) {
		tc.headCalls = 0
		writer := &validatingChunkWriter{}

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetCommonParameters(cp)
		prm.SetObjectWriter(writer)
		prm.MarkPayloadOnly()
		rng := object.NewRange()
		rng.SetOffset(3)
		rng.SetLength(17)
		prm.SetRange(rng)

		svc := newService(t)

		err = svc.Get(ctx, prm)
		require.NoError(t, err)
		require.Equal(t, 1, writer.validateCount)
		require.Zero(t, writer.writeHeaderCount)
		require.Equal(t, parentPayload[3:20], writer.buf.Bytes())
		require.Zero(t, tc.headCalls)
	})

	t.Run("extended ranged GET reuses EC part header", func(t *testing.T) {
		tc.headCalls = 0

		var w mockObjectWriter

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetCommonParameters(cp)
		prm.SetObjectWriter(&w)
		prm.SetRangeSuffix(17)

		svc := newService(t)

		err = svc.Get(ctx, prm)
		require.NoError(t, err)
		require.Zero(t, tc.headCalls)
		require.Equal(t, 1, w.writeHeaderCount)
		require.Equal(t, parentHdr, w.hdr)
		require.Equal(t, parentPayload[len(parentPayload)-17:], w.buf.Bytes())
	})

	t.Run("out-of-range does not write header", func(t *testing.T) {
		var w mockObjectWriter

		var prm Prm
		prm.WithAddress(parentAddr)
		prm.SetCommonParameters(cp)
		prm.SetObjectWriter(&w)
		rng := object.NewRange()
		rng.SetOffset(uint64(len(parentPayload)))
		rng.SetLength(1)
		prm.SetRange(rng)

		svc := newService(t)

		err = svc.Get(ctx, prm)
		require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
		require.Zero(t, w.writeHeaderCount)
		require.Zero(t, w.writeChunkCount)
	})

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

type validatingChunkWriter struct {
	validateErr      error
	validateCount    int
	writeHeaderCount int
	buf              bytes.Buffer
}

func (x *validatingChunkWriter) ValidateHeader(*object.Object) error {
	x.validateCount++
	return x.validateErr
}

func (x *validatingChunkWriter) WriteHeader(*object.Object) error {
	x.writeHeaderCount++
	return nil
}

func (x *validatingChunkWriter) WriteChunk(p []byte) error {
	_, err := x.buf.Write(p)
	return err
}

type testECServiceConn struct {
	mockKeyStorage
	parentHdr object.Object

	nodes     map[string]*Service
	headCalls int
}

func (x *testECServiceConn) InitGetObjectStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey,
	cnr cid.ID, id oid.ID, local, verifyID bool, rng *object.Range, xs []string) (object.Object, io.ReadCloser, error) {
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

	v, ok := x.nodes[string(node.Marshal())]
	if !ok {
		return object.Object{}, nil, errors.New("[test] unexpected node")
	}

	cp := newCommonParameters(local, xs)

	var w mockObjectWriter

	var prm Prm
	prm.WithAddress(oid.NewAddress(cnr, id))
	prm.SetObjectWriter(&w)
	prm.SetCommonParameters(cp)
	if rng != nil {
		prm.SetRange(rng)
		prm.MarkPayloadOnly()
	}

	if err := v.Get(ctx, prm); err != nil {
		return object.Object{}, nil, err
	}

	return w.hdr, io.NopCloser(&w.buf), nil
}

func (x *testECServiceConn) Head(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID) (object.Object, error) {
	x.headCalls++
	if ctx == nil {
		return object.Object{}, errors.New("[test] missing context")
	}
	if !pk.Equal(&x.privKey) {
		return object.Object{}, errors.New("[test] unexpected private key")
	}
	if _, ok := x.nodes[string(node.Marshal())]; !ok {
		return object.Object{}, errors.New("[test] unexpected node")
	}
	if x.parentHdr.GetID() != id || x.parentHdr.GetContainerID() != cnr {
		return object.Object{}, errors.New("[test] unexpected object requested")
	}

	return *x.parentHdr.CutPayload(), nil
}

type noopClientConstructor struct{}

func (noopClientConstructor) Get(context.Context, netmap.NodeInfo) (clientcore.MultiAddressClient, error) {
	return nil, nil
}

type mockECTransport struct {
	parts        [][]byte
	parentPldLen uint64
	partPldLen   uint64

	part0Err        error
	part0ErrAfter   bool
	part0NoHdr      bool
	part0PartialLen uint64

	beforePart0Payload func()
	onRangeStart       func(partIdx int, off, ln uint64)
	onRangeExit        func(partIdx int)

	mtx        sync.Mutex
	hdrWritten bool
	written    []byte
	writeOrder []int
	rangeCalls map[int][][2]uint64
}

func newMockECTransport(parts [][]byte, parentPldLen uint64) *mockECTransport {
	var partLen uint64
	if len(parts) > 0 {
		partLen = uint64(len(parts[0]))
	}
	return &mockECTransport{
		parts:        parts,
		parentPldLen: parentPldLen,
		partPldLen:   partLen,
		rangeCalls:   make(map[int][][2]uint64),
	}
}

func (m *mockECTransport) copyFirstPart(partInfo iec.PartInfo, interceptLens func(uint64, uint64) error) (bool, uint64, error) {
	if m.part0NoHdr {
		return false, 0, nil
	}
	if m.part0Err != nil && !m.part0ErrAfter {
		return false, 0, m.part0Err
	}

	partLen := m.partPldLen
	if err := interceptLens(partLen, m.parentPldLen); err != nil {
		return false, 0, err
	}

	if m.beforePart0Payload != nil {
		m.beforePart0Payload()
	}

	if m.part0Err != nil && m.part0ErrAfter {
		return false, 0, m.part0Err
	}

	var data []byte
	if partInfo.Index < len(m.parts) {
		data = m.parts[partInfo.Index]
	}
	if m.part0PartialLen > 0 && m.part0PartialLen < uint64(len(data)) {
		data = data[:m.part0PartialLen]
	}

	m.mtx.Lock()
	m.hdrWritten = true
	m.written = append(m.written, data...)
	m.writeOrder = append(m.writeOrder, partInfo.Index)
	m.mtx.Unlock()

	return true, uint64(len(data)), nil
}

func (m *mockECTransport) CopyLocalECPartParentHeaderAndPayload(_ context.Context, _ *engine.StorageEngine, partInfo iec.PartInfo, interceptLens func(uint64, uint64) error) (bool, uint64, error) {
	return m.copyFirstPart(partInfo, interceptLens)
}

func (m *mockECTransport) CopyRemoteECPartParentHeaderAndPayload(_ context.Context, _ clientcore.MultiAddressClient, partInfo iec.PartInfo, interceptLens func(uint64, uint64) error) (bool, uint64, error) {
	return m.copyFirstPart(partInfo, interceptLens)
}

func (m *mockECTransport) copyPartRange(ctx context.Context, partInfo iec.PartInfo, off, ln uint64, controlCh <-chan bool) (uint64, error) {
	if m.onRangeStart != nil {
		m.onRangeStart(partInfo.Index, off, ln)
	}
	if m.onRangeExit != nil {
		defer m.onRangeExit(partInfo.Index)
	}

	m.mtx.Lock()
	m.rangeCalls[partInfo.Index] = append(m.rangeCalls[partInfo.Index], [2]uint64{off, ln})
	m.mtx.Unlock()

	if controlCh != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case abort := <-controlCh:
			if abort {
				return 0, ErrAborted
			}
		}
	}

	if partInfo.Index == 0 && m.part0PartialLen > 0 {
		// Simulate partial failure where fallback nodes also have no remaining bytes.
		return 0, nil
	}

	part := m.parts[partInfo.Index]
	chunk := part[off : off+ln]

	m.mtx.Lock()
	m.written = append(m.written, chunk...)
	m.writeOrder = append(m.writeOrder, partInfo.Index)
	m.mtx.Unlock()

	return uint64(len(chunk)), nil
}

func (m *mockECTransport) CopyLocalECPartRange(ctx context.Context, _ *engine.StorageEngine, partInfo iec.PartInfo, off, ln uint64, controlCh <-chan bool) (uint64, error) {
	return m.copyPartRange(ctx, partInfo, off, ln, controlCh)
}

func (m *mockECTransport) CopyRemoteECPartRange(ctx context.Context, _ clientcore.MultiAddressClient, partInfo iec.PartInfo, off, ln uint64, controlCh <-chan bool) (uint64, error) {
	return m.copyPartRange(ctx, partInfo, off, ln, controlCh)
}

func newStreamECService(t testing.TB, rule iec.Rule, localIdx int) (*Service, []netmap.NodeInfo) {
	t.Helper()
	nodeLists, _ := testNodeMatrix(t, []int{int(rule.DataPartNum + rule.ParityPartNum)})
	nodes := nodeLists[0]

	var localPub []byte
	if localIdx >= 0 {
		localPub = nodes[localIdx].PublicKey()
	}

	svc := New(&mockNeoFSNet{localPubKey: localPub},
		WithLocalStorageEngine(nil),
		WithClientConstructor(noopClientConstructor{}),
	)

	return svc, nodes
}

func TestStreamECObject(t *testing.T) {
	ctx := context.Background()

	t.Run("early release and gate ordering", func(t *testing.T) {
		rule := iec.Rule{DataPartNum: 4, ParityPartNum: 2}
		payload := testutil.RandByteSlice(100)
		parts, _, err := iec.Encode(rule, payload)
		require.NoError(t, err)

		svc, nodes := newStreamECService(t, rule, -1)
		tr := newMockECTransport(parts, uint64(len(payload)))

		var started atomic.Int32
		allStarted := make(chan struct{})
		tr.onRangeStart = func(int, uint64, uint64) {
			if started.Add(1) == int32(rule.DataPartNum-1) {
				close(allStarted)
			}
		}
		tr.beforePart0Payload = func() {
			<-allStarted
			// Secondary parts have started fetching, but none may write before part#0 finishes.
			tr.mtx.Lock()
			writtenLen := len(tr.written)
			tr.mtx.Unlock()
			require.Zero(t, writtenLen)
		}

		err = svc.streamECObject(ctx, tr, rule, 0, nodes)
		require.NoError(t, err)
		require.Equal(t, payload, tr.written)
		require.Equal(t, []int{0, 1, 2, 3}, tr.writeOrder)
	})

	t.Run("local part#0 and remote secondary parts", func(t *testing.T) {
		rule := iec.Rule{DataPartNum: 4, ParityPartNum: 2}
		payload := testutil.RandByteSlice(37)
		parts, _, err := iec.Encode(rule, payload)
		require.NoError(t, err)

		svc, nodes := newStreamECService(t, rule, 0)
		tr := newMockECTransport(parts, uint64(len(payload)))

		err = svc.streamECObject(ctx, tr, rule, 0, nodes)
		require.NoError(t, err)
		require.Equal(t, payload, tr.written)
		require.Equal(t, []int{0, 1, 2, 3}, tr.writeOrder)
	})

	t.Run("small payloads with trailing padding", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			rule         iec.Rule
			payload      []byte
			wantRangeLen map[int]uint64
		}{
			{
				name:         "N=4 L=5",
				rule:         iec.Rule{DataPartNum: 4, ParityPartNum: 2},
				payload:      []byte{1, 2, 3, 4, 5},
				wantRangeLen: map[int]uint64{1: 2, 2: 1},
			},
			{
				name:         "N=4 L=6 last part padding only",
				rule:         iec.Rule{DataPartNum: 4, ParityPartNum: 2},
				payload:      []byte{1, 2, 3, 4, 5, 6},
				wantRangeLen: map[int]uint64{1: 2, 2: 2},
			},
			{
				name:         "N=3 L=1",
				rule:         iec.Rule{DataPartNum: 3, ParityPartNum: 1},
				payload:      []byte{42},
				wantRangeLen: map[int]uint64{},
			},
			{
				name:         "N=4 L=0 empty",
				rule:         iec.Rule{DataPartNum: 4, ParityPartNum: 2},
				payload:      nil,
				wantRangeLen: map[int]uint64{},
			},
			{
				name:         "N=1 L=5 single data part",
				rule:         iec.Rule{DataPartNum: 1, ParityPartNum: 1},
				payload:      []byte{1, 2, 3, 4, 5},
				wantRangeLen: map[int]uint64{},
			},
			{
				name:         "N=1 L=0 single data part empty",
				rule:         iec.Rule{DataPartNum: 1, ParityPartNum: 1},
				payload:      nil,
				wantRangeLen: map[int]uint64{},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parts, _, err := iec.Encode(tc.rule, tc.payload)
				require.NoError(t, err)

				svc, nodes := newStreamECService(t, tc.rule, -1)
				tr := newMockECTransport(parts, uint64(len(tc.payload)))

				err = svc.streamECObject(ctx, tr, tc.rule, 0, nodes)
				require.NoError(t, err)
				require.Equal(t, tc.payload, tr.written)

				require.Len(t, tr.rangeCalls, len(tc.wantRangeLen))
				for partIdx, wantLen := range tc.wantRangeLen {
					require.Equal(t, [][2]uint64{{0, wantLen}}, tr.rangeCalls[partIdx])
				}
			})
		}
	})

	t.Run("invalid part payload length", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			rule      iec.Rule
			partLen   uint64
			parentLen uint64
		}{
			{name: "too small", rule: iec.Rule{DataPartNum: 4, ParityPartNum: 2}, partLen: 1, parentLen: 5},
			{name: "zero part len", rule: iec.Rule{DataPartNum: 4, ParityPartNum: 2}, partLen: 0, parentLen: 5},
			{name: "zero parent len", rule: iec.Rule{DataPartNum: 4, ParityPartNum: 2}, partLen: 2, parentLen: 0},
			{name: "single data part mismatch", rule: iec.Rule{DataPartNum: 1, ParityPartNum: 1}, partLen: 3, parentLen: 5},
		} {
			t.Run(tc.name, func(t *testing.T) {
				svc, nodes := newStreamECService(t, tc.rule, -1)
				tr := newMockECTransport(nil, tc.parentLen)
				tr.partPldLen = tc.partLen

				err := svc.streamECObject(ctx, tr, tc.rule, 0, nodes)
				require.ErrorContains(t, err, "part#0:")
				require.ErrorContains(t, err, "invalid part payload length")
				require.False(t, tr.hdrWritten)
				require.Empty(t, tr.written)
				require.Empty(t, tr.rangeCalls)
			})
		}
	})

	t.Run("early returns join part goroutines", func(t *testing.T) {
		rule := iec.Rule{DataPartNum: 4, ParityPartNum: 2}
		payload := testutil.RandByteSlice(40)
		parts, _, err := iec.Encode(rule, payload)
		require.NoError(t, err)

		for _, tc := range []struct {
			name        string
			setup       func(*mockECTransport)
			waitForPart bool
			assertErr   func(*testing.T, error)
		}{
			{
				name: "part#0 ErrResponded",
				setup: func(tr *mockECTransport) {
					tr.part0Err = ErrResponded
					tr.part0ErrAfter = true
				},
				waitForPart: true,
				assertErr: func(t *testing.T, err error) {
					require.NoError(t, err)
				},
			},
			{
				name: "part#0 error",
				setup: func(tr *mockECTransport) {
					tr.part0Err = errors.New("part#0 stream failure")
					tr.part0ErrAfter = true
				},
				waitForPart: true,
				assertErr: func(t *testing.T, err error) {
					require.ErrorContains(t, err, "part#0: part#0 stream failure")
				},
			},
			{
				name: "part#0 header not copied",
				setup: func(tr *mockECTransport) {
					tr.part0NoHdr = true
				},
				assertErr: func(t *testing.T, err error) {
					var partial partialObjectCopy
					require.ErrorAs(t, err, &partial)
					require.False(t, partial.copiedHeader)
				},
			},
			{
				name: "part#0 partial payload",
				setup: func(tr *mockECTransport) {
					tr.part0PartialLen = 4
				},
				waitForPart: true,
				assertErr: func(t *testing.T, err error) {
					var partial partialObjectCopy
					require.ErrorAs(t, err, &partial)
					require.True(t, partial.copiedHeader)
					require.EqualValues(t, 4, partial.copiedPayloadLength)
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				svc, nodes := newStreamECService(t, rule, -1)
				tr := newMockECTransport(parts, uint64(len(payload)))
				tc.setup(tr)

				var active atomic.Int32
				var started atomic.Int32
				allStarted := make(chan struct{})
				tr.onRangeStart = func(partIdx int, _, _ uint64) {
					if partIdx == 0 {
						return
					}
					active.Add(1)
					if started.Add(1) == int32(rule.DataPartNum-1) {
						close(allStarted)
					}
				}
				tr.onRangeExit = func(partIdx int) {
					if partIdx == 0 {
						return
					}
					active.Add(-1)
				}
				if tc.waitForPart {
					tr.beforePart0Payload = func() {
						<-allStarted
					}
				}

				err := svc.streamECObject(ctx, tr, rule, 0, nodes)
				tc.assertErr(t, err)
				require.Zero(t, active.Load())
			})
		}
	})
}
