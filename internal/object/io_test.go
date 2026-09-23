package object_test

import (
	"bytes"
	"errors"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iiotest "github.com/nspcc-dev/neofs-node/internal/testutil/iotest"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestWriterTo_WriteTo(t *testing.T) {
	obj := objecttest.Object()
	objW := iobject.WriterTo(obj)

	t.Run("writer error", func(t *testing.T) {
		writerErr := errors.New("any writer error")
		w := iiotest.NewErrorWriter(writerErr)

		_, err := objW.WriteTo(w)
		require.ErrorIs(t, err, writerErr)
		require.EqualError(t, err, writerErr.Error())
	})

	var buf bytes.Buffer

	n, err := iobject.WriterTo(obj).WriteTo(&buf)
	require.NoError(t, err)

	exp := obj.Marshal()
	require.EqualValues(t, len(exp), n)
	require.True(t, bytes.Equal(exp, buf.Bytes()))
}
