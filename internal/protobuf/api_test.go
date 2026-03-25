package protobuf_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func testAPIVerifyFuncInvalidTag(t *testing.T, f func([]byte) error, valid ...[]byte) {
	composeWithBytes := func(b []byte) []byte {
		return slices.Concat(append(valid, b)...)
	}

	off := islices.TwoDimSliceElementCount(valid)

	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			if len(tc.buf) == 0 {
				continue
			}
			t.Run(tc.name, func(t *testing.T) {
				err := f(composeWithBytes(tc.buf))
				require.ErrorContains(t, err, "parse tag at offset "+strconv.Itoa(off))
				require.ErrorContains(t, err, "parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("invalid number", func(t *testing.T) {
		t.Run("0", func(t *testing.T) {
			err := f(composeWithBytes([]byte{2})) // 0,LEN
			require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(off)+": invalid number 0")
		})
		t.Run("negative", func(t *testing.T) {
			err := f(composeWithBytes([]byte{250, 255, 255, 255, 255, 255, 255, 255, 255, 1})) // -1,LEN
			require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(off)+": invalid number -1")
		})
	})
}

func testAPIVerifyFuncUnknownField(t *testing.T, f func([]byte) error, knownFlds ...[]byte) {
	num := protowire.Number(len(knownFlds) + 1)

	tag := protowire.AppendTag(nil, num, protowire.BytesType)

	t.Run("invalid", func(t *testing.T) {
		unknownFld := slices.Concat(tag, []byte{13}, []byte("Hello, world"))
		err := f(slices.Concat(append(knownFlds, unknownFld)...))
		require.ErrorContains(t, err, fmt.Sprintf("parse field #%d of LEN type", num))
	})

	unknownFld := slices.Concat(tag, []byte{13}, []byte("Hello, world!"))
	err := f(slices.Concat(append(knownFlds, unknownFld)...))
	require.NoError(t, err)
}

func testAPIVerifyFuncUnorderedFields(t *testing.T, f func([]byte) error, ordered ...[]byte) {
	slices.Reverse(ordered)
	err := f(slices.Concat(ordered...))
	require.NoError(t, err)
}

func testAPIVerifyFuncInvalidUint32Field(t *testing.T, f func([]byte) error, num protowire.Number, valid ...[]byte) {
	tag := protowire.AppendTag(nil, num, protowire.VarintType)

	composeWithBytes := func(b []byte) []byte {
		fld := slices.Concat(tag, b)
		return slices.Concat(append(valid, fld)...)
	}

	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				err := f(composeWithBytes(tc.buf))
				require.ErrorContains(t, err, fmt.Sprintf("parse field #%d of VARINT type: parse varint", num))
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("value overflow", func(t *testing.T) {
		err := f(composeWithBytes(uint32OverflowVarint))
		require.EqualError(t, err, fmt.Sprintf("parse field #%d of VARINT type: value 4294967296 overflows uint32", num))
	})
}

func testAPIVerifyFuncInvalidBytesField(t *testing.T, f func([]byte) error, num protowire.Number, valid ...[]byte) {
	_testAPIVerifyFuncInvalidBytesField(t, f, num, false, valid...)
}

func testAPIVerifyFuncInvalidStringField(t *testing.T, f func([]byte) error, num protowire.Number, valid ...[]byte) {
	_testAPIVerifyFuncInvalidBytesField(t, f, num, true, valid...)
}

func _testAPIVerifyFuncInvalidBytesField(t *testing.T, f func([]byte) error, num protowire.Number, isString bool, valid ...[]byte) {
	tag := protowire.AppendTag(nil, num, protowire.BytesType)

	composeWithBytes := func(b []byte) []byte {
		fld := slices.Concat(tag, b)
		return slices.Concat(append(valid, fld)...)
	}

	t.Run("invalid len", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				err := f(composeWithBytes(tc.buf))
				require.ErrorContains(t, err, fmt.Sprintf("parse field #%d of LEN type: parse varint", num))
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("buffer overflow", func(t *testing.T) {
		t.Run("int overflow", func(t *testing.T) {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

			err := f(composeWithBytes(buf[:n]))
			require.EqualError(t, err, fmt.Sprintf("parse field #%d of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int", num))
		})

		for i, tc := range varintTestcases {
			if tc.val == 0 || tc.val > 1<<20 {
				continue
			}

			err := f(composeWithBytes(slices.Concat(tc.buf, make([]byte, tc.val-1))))
			require.EqualError(t, err, fmt.Sprintf("parse field #%d of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", num), i)
		}
	})

	if !isString {
		return
	}

	t.Run("invalid UTF-8", func(t *testing.T) {
		fld := protowire.AppendBytes(nil, invalidUTF8)
		err := f(composeWithBytes(fld))
		require.EqualError(t, err, fmt.Sprintf("string field #%d contains invalid UTF-8", num))
	})
}

func TestVerifyAPIVersion(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, iprotobuf.VerifyAPIVersion(nil))
		require.NoError(t, iprotobuf.VerifyAPIVersion([]byte{}))
	})

	majorFld := []byte{iprotobuf.TagVarint1, 229, 194, 133, 172, 11} // 3045155173
	minorFld := []byte{iprotobuf.TagVarint2, 193, 231, 182, 214, 13} // 3670913985

	t.Run("invalid tag", func(t *testing.T) {
		testAPIVerifyFuncInvalidTag(t, iprotobuf.VerifyAPIVersion, majorFld, minorFld)
	})

	t.Run("invalid major field", func(t *testing.T) {
		testAPIVerifyFuncInvalidUint32Field(t, iprotobuf.VerifyAPIVersion, protorefs.FieldVersionMajor, minorFld)
	})

	t.Run("invalid minor field", func(t *testing.T) {
		testAPIVerifyFuncInvalidUint32Field(t, iprotobuf.VerifyAPIVersion, protorefs.FieldVersionMinor, majorFld)
	})

	t.Run("unknown field", func(t *testing.T) {
		testAPIVerifyFuncUnknownField(t, iprotobuf.VerifyAPIVersion, majorFld, minorFld)
	})

	t.Run("unordered fields", func(t *testing.T) {
		testAPIVerifyFuncUnorderedFields(t, iprotobuf.VerifyAPIVersion, majorFld, minorFld)
	})

	err := iprotobuf.VerifyAPIVersion(slices.Concat(majorFld, minorFld))
	require.NoError(t, err)
}

func TestVerifyStatusDetail(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, iprotobuf.VerifyStatusDetail(nil))
		require.NoError(t, iprotobuf.VerifyStatusDetail([]byte{}))
	})

	idFld := []byte{iprotobuf.TagVarint1, 249, 163, 142, 245, 11} // 3198390777
	valueFld := slices.Concat([]byte{iprotobuf.TagBytes2, 13}, []byte("Hello, world!"))

	t.Run("invalid tag", func(t *testing.T) {
		testAPIVerifyFuncInvalidTag(t, iprotobuf.VerifyStatusDetail, idFld, valueFld)
	})

	t.Run("invalid ID field", func(t *testing.T) {
		testAPIVerifyFuncInvalidUint32Field(t, iprotobuf.VerifyStatusDetail, protostatus.FieldStatusDetailID, valueFld)
	})

	t.Run("invalid value field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyStatusDetail, protostatus.FieldStatusDetailValue, idFld)
	})

	t.Run("unknown field", func(t *testing.T) {
		testAPIVerifyFuncUnknownField(t, iprotobuf.VerifyStatusDetail, idFld, valueFld)
	})

	t.Run("unordered fields", func(t *testing.T) {
		testAPIVerifyFuncUnorderedFields(t, iprotobuf.VerifyStatusDetail, idFld, valueFld)
	})

	err := iprotobuf.VerifyStatusDetail(slices.Concat(idFld, valueFld))
	require.NoError(t, err)
}

func TestVerifyXHeader(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, iprotobuf.VerifyStatusDetail(nil))
		require.NoError(t, iprotobuf.VerifyStatusDetail([]byte{}))
	})

	keyFld := slices.Concat([]byte{iprotobuf.TagBytes1, 13}, []byte("Hello, world!"))
	valueFld := slices.Concat([]byte{iprotobuf.TagBytes2, 3}, []byte("foo"))

	t.Run("invalid tag", func(t *testing.T) {
		testAPIVerifyFuncInvalidTag(t, iprotobuf.VerifyXHeader, keyFld, valueFld)
	})

	t.Run("invalid key field", func(t *testing.T) {
		testAPIVerifyFuncInvalidStringField(t, iprotobuf.VerifyXHeader, protosession.FieldXHeaderKey, valueFld)
	})

	t.Run("invalid value field", func(t *testing.T) {
		testAPIVerifyFuncInvalidStringField(t, iprotobuf.VerifyXHeader, protosession.FieldXHeaderValue, keyFld)
	})

	t.Run("unknown field", func(t *testing.T) {
		testAPIVerifyFuncUnknownField(t, iprotobuf.VerifyXHeader, keyFld, valueFld)
	})

	t.Run("unordered fields", func(t *testing.T) {
		testAPIVerifyFuncUnorderedFields(t, iprotobuf.VerifyXHeader, keyFld, valueFld)
	})

	err := iprotobuf.VerifyXHeader(slices.Concat(keyFld, valueFld))
	require.NoError(t, err)
}

func TestVerifyObjectSplitInfo(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, iprotobuf.VerifyObjectSplitInfo(nil))
		require.NoError(t, iprotobuf.VerifyObjectSplitInfo([]byte{}))
	})

	splitIDFld := slices.Concat([]byte{iprotobuf.TagBytes1, 16}, testutil.RandByteSlice(16))
	lastIDFld := slices.Concat([]byte{iprotobuf.TagBytes2, 34, iprotobuf.TagBytes1, 32}, testutil.RandByteSlice(32))
	linkIDFld := slices.Concat([]byte{iprotobuf.TagBytes3, 34, iprotobuf.TagBytes1, 32}, testutil.RandByteSlice(32))
	firstIDFld := slices.Concat([]byte{iprotobuf.TagBytes4, 34, iprotobuf.TagBytes1, 32}, testutil.RandByteSlice(32))

	t.Run("invalid tag", func(t *testing.T) {
		testAPIVerifyFuncInvalidTag(t, iprotobuf.VerifyObjectSplitInfo, splitIDFld, lastIDFld, linkIDFld, firstIDFld)
	})

	t.Run("invalid split ID field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyObjectSplitInfo, protoobject.FieldSplitInfoSplitID, lastIDFld, linkIDFld, firstIDFld)
	})

	t.Run("invalid last ID field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyObjectSplitInfo, protoobject.FieldSplitInfoLastPart, splitIDFld, linkIDFld, firstIDFld)
	})

	t.Run("invalid link ID field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyObjectSplitInfo, protoobject.FieldSplitInfoLink, splitIDFld, lastIDFld, firstIDFld)
	})

	t.Run("invalid first ID field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyObjectSplitInfo, protoobject.FieldSplitInfoFirstPart, splitIDFld, lastIDFld, linkIDFld)
	})

	t.Run("unknown field", func(t *testing.T) {
		testAPIVerifyFuncUnknownField(t, iprotobuf.VerifyObjectSplitInfo, splitIDFld, lastIDFld, linkIDFld, firstIDFld)
	})

	t.Run("unordered fields", func(t *testing.T) {
		testAPIVerifyFuncUnorderedFields(t, iprotobuf.VerifyObjectSplitInfo, splitIDFld, lastIDFld, linkIDFld, firstIDFld)
	})

	err := iprotobuf.VerifyObjectSplitInfo(slices.Concat(splitIDFld, lastIDFld, linkIDFld, firstIDFld))
	require.NoError(t, err)
}

func TestVerifyObjectID(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, iprotobuf.VerifyObjectID(nil))
		require.NoError(t, iprotobuf.VerifyObjectID([]byte{}))
	})

	valueFld := slices.Concat([]byte{iprotobuf.TagBytes1, 32}, testutil.RandByteSlice(32))

	t.Run("invalid tag", func(t *testing.T) {
		testAPIVerifyFuncInvalidTag(t, iprotobuf.VerifyObjectID, valueFld)
	})

	t.Run("invalid value field", func(t *testing.T) {
		testAPIVerifyFuncInvalidBytesField(t, iprotobuf.VerifyObjectID, protorefs.FieldObjectIDValue, valueFld)
	})

	t.Run("unknown field", func(t *testing.T) {
		testAPIVerifyFuncUnknownField(t, iprotobuf.VerifyObjectID, valueFld)
	})

	t.Run("unordered fields", func(t *testing.T) {
		testAPIVerifyFuncUnorderedFields(t, iprotobuf.VerifyObjectID, valueFld)
	})

	err := iprotobuf.VerifyObjectID(valueFld)
	require.NoError(t, err)
}

func TestGetStatusCodeFromResponseMetaHeader(t *testing.T) {
	assertOK := func(t *testing.T, m *protosession.ResponseMetaHeader, code uint32) {
		buf := make([]byte, m.MarshaledSize())
		m.MarshalStable(buf)
		got, err := iprotobuf.GetStatusCodeFromResponseMetaHeader(buf)
		require.NoError(t, err)
		require.EqualValues(t, code, got)
	}

	m := &protosession.ResponseMetaHeader{
		Version: &protorefs.Version{
			Major: 3651676384,
			Minor: 2829345803,
		},
		Epoch: 10699904184716558895,
		Ttl:   1657590594,
		XHeaders: []*protosession.XHeader{
			{Key: "key1", Value: ""},
			{Key: "", Value: "value2"},
			{Key: "key3", Value: "value3"},
			{Key: "", Value: ""},
		},
		Status: &protostatus.Status{
			Code:    2606808419,
			Message: "any root status message",
			Details: []*protostatus.Status_Detail{
				{Id: 1869765515, Value: []byte("foo")},
				{Id: 2095463591, Value: []byte("bar")},
			},
		},
	}

	assertOK(t, m, 2606808419)

	for range 5 {
		m = &protosession.ResponseMetaHeader{
			Origin: m,
			Status: &protostatus.Status{
				Code: rand.Uint32(),
			},
		}
	}

	assertOK(t, m, 2606808419)
}
