package ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfo(t *testing.T) {
	s := Info{}

	n1 := Node{}
	n1.SetKey([]byte{1, 2, 3})

	n2 := Node{}
	n2.SetKey([]byte{4, 5, 6})

	nodes := []Node{
		n1,
		n2,
	}
	s.SetNodes(nodes)

	require.Equal(t, nodes, s.Nodes())
}
