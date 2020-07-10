package acl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryEACLValue(t *testing.T) {
	s := BinaryEACLValue{}

	eacl := []byte{1, 2, 3}
	s.SetEACL(eacl)
	require.Equal(t, eacl, s.EACL())

	sig := []byte{4, 5, 6}
	s.SetSignature(sig)
	require.Equal(t, sig, s.Signature())

	data, err := s.MarshalBinary()
	require.NoError(t, err)

	s2 := BinaryEACLValue{}
	require.NoError(t, s2.UnmarshalBinary(data))

	require.Equal(t, s, s2)
}
