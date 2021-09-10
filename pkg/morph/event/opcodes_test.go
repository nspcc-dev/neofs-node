package event

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/stretchr/testify/require"
)

func TestBytesFromOpcode(t *testing.T) {
	tests := [...][]byte{
		[]byte("test"),
		[]byte("test test"),
		[]byte(""),
		[]byte("1"),
	}

	bw := io.NewBufBinWriter()

	for _, test := range tests {
		emit.Bytes(bw.BinWriter, test)
	}

	var (
		ctx = vm.NewContext(bw.Bytes())

		op Op

		gotBytes []byte
		err      error
	)

	for _, test := range tests {
		op = getNextOp(ctx)

		gotBytes, err = BytesFromOpcode(op)

		require.NoError(t, err)
		require.Equal(t, test, gotBytes)
	}
}

func TestIntFromOpcode(t *testing.T) {
	tests := [...]int64{
		-1,
		-5,
		15,
		16,
		1_000_000,
	}

	bw := io.NewBufBinWriter()

	for _, test := range tests {
		emit.Int(bw.BinWriter, test)
	}

	var (
		ctx = vm.NewContext(bw.Bytes())

		op Op

		gotInt int64
		err    error
	)

	for _, test := range tests {
		op = getNextOp(ctx)

		gotInt, err = IntFromOpcode(op)

		require.NoError(t, err)
		require.Equal(t, test, gotInt)
	}
}

func getNextOp(ctx *vm.Context) (op Op) {
	op.code, op.param, _ = ctx.Next()
	return
}
