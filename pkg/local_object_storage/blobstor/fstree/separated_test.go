package fstree

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestSeparatedObject(t *testing.T) {
	newSeparatedTestObject := func(payload []byte) ([]byte, []byte) {
		obj := objecttest.Object()
		obj.SetPayload(payload)

		canonical := obj.Marshal()
		return canonical, separateObject(canonical)
	}

	t.Run("round-trip", func(t *testing.T) {
		payload := []byte("payload")
		canonical, separated := newSeparatedTestObject(payload)

		headerLen, payloadLen := parseSeparatedPrefix(separated)
		require.NotZero(t, headerLen)
		require.EqualValues(t, len(payload), payloadLen)
		require.Equal(t, canonical[:headerLen], separated[separatedDataOff:separatedDataOff+int(headerLen)])

		restored, ok, err := restoreSeparatedObject(separated)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, canonical, restored)
	})

	t.Run("legacy canonical", func(t *testing.T) {
		tree := setupFSTree(t)
		obj := objecttest.Object()
		obj.SetPayload([]byte("legacy payload"))

		path := tree.treePath(obj.Address())
		require.NoError(t, os.MkdirAll(filepath.Dir(path), tree.Permissions))
		require.NoError(t, tree.writer.writeData(obj.Address().Object(), path, obj.Marshal()))

		got, reader, err := tree.GetStream(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), got)
		assertPayloadSeeker(t, reader, obj.Payload())
		require.NoError(t, reader.Close())
	})

	t.Run("put batch", func(t *testing.T) {
		tree := setupFSTree(t)
		obj := objecttest.Object()
		payload := []byte("payload stored in a batch")
		obj.SetPayload(payload)
		require.NoError(t, tree.PutBatch(map[oid.Address][]byte{obj.Address(): obj.Marshal()}))

		raw := readRawObjectFile(t, tree, obj.Address())
		entry, _, err := compressedEntryForAddress(raw, obj.Address().Object())
		require.NoError(t, err)
		headerLen, payloadLen := parseSeparatedPrefix(entry.data)
		require.NotZero(t, headerLen)
		require.EqualValues(t, len(payload), payloadLen)

		_, _, stream, err := tree.GetRangeStream(obj.Address(), common.NewPayloadRange(2, 7), false)
		require.NoError(t, err)
		got, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.Equal(t, payload[2:9], got)
	})

	t.Run("malformed", func(t *testing.T) {
		_, separated := newSeparatedTestObject([]byte("payload"))

		_, ok, err := restoreSeparatedObject(separated[:len(separated)-1])
		require.True(t, ok)
		require.Error(t, err)

		corrupted := append([]byte(nil), separated...)
		binary.BigEndian.PutUint64(corrupted[separatedHeaderLenOff:], ^uint64(0))
		_, ok, err = restoreSeparatedObject(corrupted)
		require.True(t, ok)
		require.Error(t, err)
	})
}
