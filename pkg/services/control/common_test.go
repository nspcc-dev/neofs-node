package control_test

import (
	"crypto/rand"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type protoMessage interface {
	StableMarshal([]byte) ([]byte, error)
	proto.Message
}

func testStableMarshal(t *testing.T, m1, m2 protoMessage, cmp func(m1, m2 protoMessage) bool) {
	data, err := m1.StableMarshal(nil)
	require.NoError(t, err)

	require.NoError(t, proto.Unmarshal(data, m2))

	require.True(t, cmp(m1, m2))
}

func testData(sz int) []byte {
	d := make([]byte, sz)

	_, _ = rand.Read(d)

	return d
}

func testString() string {
	return base58.Encode(testData(10))
}
