package client

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInitEndpoints(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	ee := make([]Endpoint, 100)
	for i := range ee {
		ee[i].Priority = rand.Int()
	}

	var eeInternal endpoints
	eeInternal.init(ee)

	prevValue := eeInternal.list[0].Priority

	for _, e := range eeInternal.list {
		require.True(t, prevValue <= e.Priority)

		prevValue = e.Priority
	}
}
