package implementations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticContractClient(t *testing.T) {
	s := new(StaticContractClient)

	require.NotPanics(t, func() {
		_, _ = s.TestInvoke("")
	})

	require.NotPanics(t, func() {
		_ = s.Invoke("")
	})
}
