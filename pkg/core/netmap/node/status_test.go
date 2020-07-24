package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatus_Full(t *testing.T) {
	st := new(Status)

	st.SetFull()
	require.True(t, st.Full())

	st.ResetFull()
	require.False(t, st.Full())
}
