package container

import (
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestCalculateID(t *testing.T) {
	_, err := CalculateID(nil)
	require.True(t, errors.Is(err, ErrNilContainer))

	cnr := new(Container)
	cnr.SetBasicACL(basic.FromUint32(1))
	cnr.SetOwnerID(OwnerID{1, 2, 3})
	cnr.SetSalt([]byte{4, 5, 6})

	id1, err := CalculateID(cnr)
	require.NoError(t, err)

	data, err := cnr.MarshalBinary()
	require.NoError(t, err)

	sh := sha256.Sum256(data)

	require.Equal(t, id1.Bytes(), sh[:])

	// change the container
	cnr.SetSalt(append(cnr.Salt(), 1))

	id2, err := CalculateID(cnr)
	require.NoError(t, err)

	require.NotEqual(t, id1, id2)
}
