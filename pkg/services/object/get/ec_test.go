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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
				require.ErrorContains(t, err, "EC rule index must be set in a correct EC part GET request")
			}},
			{name: "empty rule idx", xs: []string{
				"__NEOFS__EC_RULE_IDX", "", "__NEOFS__EC_PART_IDX", "0",
			}, assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "EC rule index must be set in a correct EC part GET request")
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
		require.EqualError(t, err, fmt.Sprintf("invalid request: missing %s and calculating node's EC part finished with error: %s", object.AttributeECPartIndex, placementErr))
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

	parentPayload := testutil.RandByteSlice(4 << 10)

	// parentHdr's address is derived from its real content (rather than
	// picked upfront) so that the parent header embedded in each EC part -
	// which readers must now verify actually hashes to the address they
	// requested - is genuinely self-consistent, matching how an honest
	// sender populates it.
	var parentHdr object.Object
	parentHdr.SetContainerID(cnr)
	parentHdr.SetAttributes(
		object.NewAttribute("parent_attr1", "parent_val1"),
		object.NewAttribute("parent_attr2", "parent_val2"),
	)
	parentHdr.SetPayloadSize(uint64(len(parentPayload)))
	require.NoError(t, parentHdr.CalculateAndSetID())
	parentID := parentHdr.GetID()
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

	cp := newCommonParameters(false)

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

	t.Run("ranged GET without part zero", func(t *testing.T) {
		partLen := len(parts[0])
		for _, tc := range []struct {
			name string
			off  int
		}{
			{name: "inside missing part", off: 10},
			{name: "inside available part", off: partLen + 10},
			{name: "across part boundary", off: partLen - 10},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for i := range nodeSvcs {
					storage := &mockLocalObjects{}
					if i != 0 {
						storage.getECPart = map[getECPartKey]getECPartValue{
							{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}: {
								hdr: *partObjs[i].CutPayload(),
								pld: parts[i],
								rdr: io.NopCloser(bytes.NewReader(parts[i])),
							},
						}
					}
					previous := nodeSvcs[i].localObjects
					t.Cleanup(func() { nodeSvcs[i].localObjects = previous })
					nodeSvcs[i].localObjects = storage
				}

				const length = 20
				var w mockObjectWriter
				var prm Prm
				prm.WithAddress(parentAddr)
				prm.SetCommonParameters(cp)
				prm.SetObjectWriter(&w)
				prm.MarkPayloadOnly()
				rng := object.NewRange()
				rng.SetOffset(uint64(tc.off))
				rng.SetLength(length)
				prm.SetRange(rng)

				require.NoError(t, newService(t).Get(ctx, prm))
				require.Equal(t, parentPayload[tc.off:tc.off+length], w.buf.Bytes())
			})
		}

		t.Run("closes non-zero header stream", func(t *testing.T) {
			prev := nodeSvcs[0].localObjects
			t.Cleanup(func() { nodeSvcs[0].localObjects = prev })
			nodeSvcs[0].localObjects = &mockLocalObjects{}

			var closer mockIOCloser
			svc := New(&mockNeoFSNet{
				getNodesForObject: getNodesMap,
				localPubKey:       nodeLists[0][1].PublicKey(),
			})
			svc.keyStore = tc
			svc.conns = tc
			svc.localObjects = &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: 1}}: {
						hdr: *partObjs[1].CutPayload(),
						pld: parts[1],
						rdr: ioReadCloser{Reader: bytes.NewReader(parts[1]), Closer: &closer},
					},
				},
			}

			off := partLen + 10
			const length = 20
			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(parentAddr)
			prm.SetCommonParameters(cp)
			prm.SetObjectWriter(&w)
			prm.MarkPayloadOnly()
			rng := object.NewRange()
			rng.SetOffset(uint64(off))
			rng.SetLength(length)
			prm.SetRange(rng)

			require.NoError(t, svc.Get(ctx, prm))
			require.Equal(t, parentPayload[off:off+length], w.buf.Bytes())
			require.EqualValues(t, 1, closer.count.Load())
		})

		t.Run("stops on non-retryable part zero error", func(t *testing.T) {
			prev0, prev1 := nodeSvcs[0].localObjects, nodeSvcs[1].localObjects
			t.Cleanup(func() {
				nodeSvcs[0].localObjects = prev0
				nodeSvcs[1].localObjects = prev1
			})
			nodeSvcs[0].localObjects = &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: 0}}: {
						err: apistatus.ErrObjectAlreadyRemoved,
					},
				},
			}
			part1Storage := &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: 1}}: {
						hdr: *partObjs[1].CutPayload(),
						pld: parts[1],
						rdr: io.NopCloser(bytes.NewReader(parts[1])),
					},
				},
			}
			nodeSvcs[1].localObjects = part1Storage

			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(parentAddr)
			prm.SetCommonParameters(cp)
			prm.SetObjectWriter(&w)
			prm.MarkPayloadOnly()
			rng := object.NewRange()
			rng.SetOffset(10)
			rng.SetLength(20)
			prm.SetRange(rng)

			require.ErrorIs(t, newService(t).Get(ctx, prm), apistatus.ErrObjectAlreadyRemoved)
			require.Zero(t, part1Storage.getECPartCount.Load())
		})

		t.Run("size-split linker from non-zero part", func(t *testing.T) {
			var splitParentHdr object.Object
			splitParentHdr.SetContainerID(cnr)
			splitParentHdr.SetPayloadSize(uint64(len(parentPayload)))
			require.NoError(t, splitParentHdr.CalculateAndSetID())
			splitParentID := splitParentHdr.GetID()
			splitParentAddr := oid.NewAddress(cnr, splitParentID)

			var child object.MeasuredObject
			child.SetObjectID(parentID)
			child.SetObjectSize(uint32(len(parentPayload)))
			var link object.Link
			link.SetObjects([]object.MeasuredObject{child})

			var linker object.Object
			linker.SetContainerID(cnr)
			linker.SetType(object.TypeLink)
			linker.SetParent(&splitParentHdr)
			linker.WriteLink(link)
			linker.SetPayloadSize(uint64(len(linker.Payload())))
			require.NoError(t, linker.CalculateAndSetID())

			nodesMap := map[oid.Address]getNodesForObjectValue{
				parentAddr:      {nodeSets: nodeLists, ecRules: []iec.Rule{rule}},
				splitParentAddr: {nodeSets: nodeLists, ecRules: []iec.Rule{rule}},
			}

			var closer mockIOCloser
			svc := New(&mockNeoFSNet{
				getNodesForObject: nodesMap,
				localPubKey:       nodeLists[0][1].PublicKey(),
			}, WithMaxObjectSizeSource(testMaxObjectSize(linker.PayloadSize())))
			svc.keyStore = tc
			svc.conns = tc
			svc.localObjects = &mockLocalObjects{
				getECPart: map[getECPartKey]getECPartValue{
					{cnr: cnr, parent: splitParentID, pi: iec.PartInfo{Index: 1}}: {
						hdr: *linker.CutPayload(),
						rdr: ioReadCloser{Reader: bytes.NewReader(linker.Payload()), Closer: &closer},
					},
					{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: 1}}: {
						hdr: *partObjs[1].CutPayload(),
						pld: parts[1],
						rdr: io.NopCloser(bytes.NewReader(parts[1])),
					},
				},
			}

			off := partLen + 10
			const length = 20
			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(splitParentAddr)
			prm.SetCommonParameters(cp)
			prm.SetObjectWriter(&w)
			prm.MarkPayloadOnly()
			rng := object.NewRange()
			rng.SetOffset(uint64(off))
			rng.SetLength(length)
			prm.SetRange(rng)

			require.NoError(t, svc.Get(ctx, prm))
			require.Equal(t, parentPayload[off:off+length], w.buf.Bytes())
			require.EqualValues(t, 1, closer.count.Load())
		})

		t.Run("extended ranged GET", func(t *testing.T) {
			for i := range nodeSvcs {
				storage := &mockLocalObjects{}
				if i != 0 {
					storage.getECPart = map[getECPartKey]getECPartValue{
						{cnr: cnr, parent: parentID, pi: iec.PartInfo{Index: i}}: {
							hdr: *partObjs[i].CutPayload(),
							pld: parts[i],
							rdr: io.NopCloser(bytes.NewReader(parts[i])),
						},
					}
				}
				previous := nodeSvcs[i].localObjects
				t.Cleanup(func() { nodeSvcs[i].localObjects = previous })
				nodeSvcs[i].localObjects = storage
			}

			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(parentAddr)
			prm.SetCommonParameters(cp)
			prm.SetObjectWriter(&w)
			rng := object.NewRange()
			rng.SetOffset(10)
			rng.SetLength(20)
			prm.SetRange(rng)

			require.NoError(t, newService(t).Get(ctx, prm))
			require.Equal(t, 1, w.writeHeaderCount)
			require.Equal(t, parentHdr, w.hdr)
			require.Equal(t, parentPayload[10:30], w.buf.Bytes())
		})

		t.Run("all parts missing", func(t *testing.T) {
			for i := range nodeSvcs {
				previous := nodeSvcs[i].localObjects
				t.Cleanup(func() { nodeSvcs[i].localObjects = previous })
				nodeSvcs[i].localObjects = &mockLocalObjects{}
			}

			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(parentAddr)
			prm.SetCommonParameters(cp)
			prm.SetObjectWriter(&w)
			prm.MarkPayloadOnly()
			rng := object.NewRange()
			rng.SetOffset(10)
			rng.SetLength(20)
			prm.SetRange(rng)

			require.ErrorIs(t, newService(t).Get(ctx, prm), apistatus.ErrObjectNotFound)
		})
	})

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

// ecLinkerScene is a size-split object within a container with a single EC
// rule: a parent, the two children it splits into, and the linker listing
// them. Everything is served locally by one node, which keeps the fetch
// deterministic; the linker paths do not depend on part counts.
//
// Only the linker's address is placed in the netmap, since the tests below
// request the linker itself rather than the object it links.
type ecLinkerScene struct {
	cnr        cid.ID
	linker     object.Object
	linkerAddr oid.Address
	childPlds  [][]byte
	svc        *Service
}

func newECLinkerScene(t testing.TB) *ecLinkerScene {
	cnr := cidtest.ID()
	rule := iec.Rule{DataPartNum: 1}

	childPlds := [][]byte{[]byte("AAAAAAAAAA"), []byte("BBBBBBBBBB")}
	children := make([]object.Object, len(childPlds))
	for i := range children {
		children[i].SetContainerID(cnr)
		children[i].SetPayload(childPlds[i])
		children[i].SetPayloadSize(uint64(len(childPlds[i])))
		children[i].CalculateAndSetPayloadChecksum()
		require.NoError(t, children[i].CalculateAndSetID())
	}

	var parentHdr object.Object
	parentHdr.SetContainerID(cnr)
	parentHdr.SetPayloadSize(uint64(len(childPlds[0]) + len(childPlds[1])))
	require.NoError(t, parentHdr.CalculateAndSetID())

	linkChildren := make([]object.MeasuredObject, len(children))
	for i := range children {
		linkChildren[i].SetObjectID(children[i].GetID())
		linkChildren[i].SetObjectSize(uint32(len(childPlds[i])))
	}
	var link object.Link
	link.SetObjects(linkChildren)

	var linker object.Object
	linker.SetContainerID(cnr)
	linker.SetType(object.TypeLink)
	linker.SetParent(&parentHdr)
	linker.WriteLink(link)
	linker.SetPayloadSize(uint64(len(linker.Payload())))
	require.NoError(t, linker.CalculateAndSetID())
	require.NotEqual(t, parentHdr.GetID(), linker.GetID())

	linkerAddr := oid.NewAddress(cnr, linker.GetID())
	nodeLists, _ := testNodeMatrix(t, []int{int(rule.DataPartNum)})

	svc := New(&mockNeoFSNet{
		getNodesForObject: map[oid.Address]getNodesForObjectValue{
			linkerAddr: {nodeSets: nodeLists, ecRules: []iec.Rule{rule}},
		},
		localPubKey: nodeLists[0][0].PublicKey(),
	}, WithMaxObjectSizeSource(testMaxObjectSize(linker.PayloadSize())))
	svc.keyStore = &mockKeyStorage{privKey: neofscryptotest.ECDSAPrivateKey()}

	local := &mockLocalObjects{getECPart: make(map[getECPartKey]getECPartValue)}
	addLocal := func(id oid.ID, hdr object.Object, pld []byte) {
		local.getECPart[getECPartKey{cnr: cnr, parent: id, pi: iec.PartInfo{}}] = getECPartValue{
			hdr: *hdr.CutPayload(),
			pld: pld,
			rdr: io.NopCloser(bytes.NewReader(pld)),
		}
	}
	addLocal(linker.GetID(), linker, linker.Payload())
	for i := range children {
		addLocal(children[i].GetID(), children[i], childPlds[i])
	}
	svc.localObjects = local

	return &ecLinkerScene{cnr: cnr, linker: linker, linkerAddr: linkerAddr, childPlds: childPlds, svc: svc}
}

// A size-split linker is an addressable object of its own, and resolving an EC
// part for its ID yields the linker itself (see [metabase.DB.ResolveECPart]).
// Its parent header is then the size-split parent it links, not the requested
// object, so it must not be verified against the requested ID.
func TestService_Get_EC_LinkerByOwnID(t *testing.T) {
	sc := newECLinkerScene(t)

	var w mockObjectWriter

	var prm Prm
	prm.WithAddress(sc.linkerAddr)
	prm.SetCommonParameters(newCommonParameters(false))
	prm.SetObjectWriter(&w)

	require.NoError(t, sc.svc.Get(context.Background(), prm))
	require.Equal(t, *sc.linker.CutPayload(), w.hdr)
	require.Equal(t, sc.linker.Payload(), w.buf.Bytes())
}

// Ranged GET has no counterpart of the above: copyECObjectRangeByRule treats
// every linker it receives as the size-split linker of the requested object,
// so a range addressed to a linker's own ID used to be resolved against the
// size-split parent and answered with that object's payload. Nothing serves a
// range of a linker's own payload, so the request must fail instead.
func TestService_Get_EC_RangeByLinkerIDIsNotServedFromParent(t *testing.T) {
	sc := newECLinkerScene(t)

	w := &validatingChunkWriter{}

	var prm Prm
	prm.WithAddress(sc.linkerAddr)
	prm.SetCommonParameters(newCommonParameters(false))
	prm.SetObjectWriter(w)
	prm.MarkPayloadOnly()
	rng := object.NewRange()
	rng.SetLength(uint64(len(sc.childPlds[0])))
	prm.SetRange(rng)

	err := sc.svc.Get(context.Background(), prm)
	require.ErrorIs(t, err, errParentHeaderIDMismatch)
	require.NotEqual(t, sc.childPlds[0], w.buf.Bytes())
	require.Zero(t, w.buf.Len())
}

func TestService_Get_EC_LinkerAbovePayloadLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranged bool
	}{
		{name: "full"},
		{name: "ranged", ranged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := newECLinkerScene(t)
			sc.svc.maxObjectSizeSrc = testMaxObjectSize(sc.linker.PayloadSize() - 1)

			local := sc.svc.localObjects.(*mockLocalObjects)
			key := getECPartKey{cnr: sc.cnr, parent: sc.linkerAddr.Object()}
			part := local.getECPart[key]
			body := bytes.NewReader(sc.linker.Payload())
			part.rdr = io.NopCloser(body)
			local.getECPart[key] = part

			var w mockObjectWriter
			var prm Prm
			prm.WithAddress(sc.linkerAddr)
			prm.SetCommonParameters(newCommonParameters(false))
			prm.SetObjectWriter(&w)
			if tc.ranged {
				prm.MarkPayloadOnly()
				rng := object.NewRange()
				rng.SetLength(1)
				prm.SetRange(rng)
			}

			require.Error(t, sc.svc.Get(context.Background(), prm))
			require.Equal(t, len(sc.linker.Payload()), body.Len())
			require.Zero(t, w.buf.Len())
		})
	}
}

type testMaxObjectSize uint64

func (s testMaxObjectSize) MaxObjectSize() uint64 { return uint64(s) }

func TestReadSizeSplitLinkerPayload(t *testing.T) {
	const limit testMaxObjectSize = 4
	svc := New(unimplementedNeoFSNet{}, WithMaxObjectSizeSource(limit))

	t.Run("at limit", func(t *testing.T) {
		r := bytes.NewBufferString("linkextra")
		payload, err := svc.readSizeSplitLinkerPayload(uint64(limit), r)
		require.NoError(t, err)
		require.Equal(t, []byte("link"), payload)
		require.Equal(t, "extra", r.String())
	})
	t.Run("above limit", func(t *testing.T) {
		r := bytes.NewBufferString("link!")
		_, err := svc.readSizeSplitLinkerPayload(uint64(limit)+1, r)
		require.ErrorContains(t, err, "exceeds limit")
		require.Equal(t, "link!", r.String())
	})
	t.Run("zero limit", func(t *testing.T) {
		svc := New(unimplementedNeoFSNet{}, WithMaxObjectSizeSource(testMaxObjectSize(0)))
		r := bytes.NewBufferString("link")
		_, err := svc.readSizeSplitLinkerPayload(uint64(limit), r)
		require.ErrorContains(t, err, "maximum object size is unavailable")
		require.Equal(t, "link", r.String())
	})
	t.Run("short read", func(t *testing.T) {
		_, err := svc.readSizeSplitLinkerPayload(uint64(limit), bytes.NewBufferString("li"))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
	t.Run("canceled", func(t *testing.T) {
		_, err := svc.readSizeSplitLinkerPayload(uint64(limit), iotest.ErrReader(status.Error(codes.Canceled, "canceled")))
		require.ErrorIs(t, err, context.Canceled)
	})
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
	cnr cid.ID, id oid.ID, local, verifyID bool, rng *object.Range, pi iec.PartInfo) (object.Object, io.ReadCloser, error) {
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

	cp := newCommonParameters(local)

	var w mockObjectWriter

	var prm Prm
	prm.WithAddress(oid.NewAddress(cnr, id))
	prm.SetObjectWriter(&w)
	prm.SetCommonParameters(cp)
	prm.ECPartSelector = &pi
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
