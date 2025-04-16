package crypto_test

import (
	"fmt"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/stretchr/testify/require"
)

func TestErrUnsupportedScheme(t *testing.T) {
	var target icrypto.ErrUnsupportedScheme

	err := fmt.Errorf("some context: %w", icrypto.ErrUnsupportedScheme(neofscrypto.N3))
	require.EqualError(t, err, "some context: unsupported scheme N3")
	require.ErrorAs(t, err, &target)
	require.EqualValues(t, neofscrypto.N3, target)

	err = fmt.Errorf("some context: %w", icrypto.ErrUnsupportedScheme(4))
	require.EqualError(t, err, "some context: unsupported scheme 4")
	require.ErrorAs(t, err, &target)
	require.EqualValues(t, 4, target)
}
