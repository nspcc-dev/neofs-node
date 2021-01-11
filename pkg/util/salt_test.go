package util_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/stretchr/testify/require"
)

func randData(sz int) []byte {
	data := make([]byte, sz)

	_, _ = rand.Read(data)

	return data
}

func TestSaltWriter_Write(t *testing.T) {
	salt := randData(4)
	data := randData(15)
	buf := bytes.NewBuffer(nil)

	w := util.NewSaltingWriter(buf, salt)

	_, err := w.Write(data)
	require.NoError(t, err)

	require.Equal(t,
		buf.Bytes(),
		util.SaltXOR(data, salt),
	)
}
