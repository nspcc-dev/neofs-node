package getsvc

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func Test_RequiredChildren(t *testing.T) {
	cases := []struct {
		name          string
		childPayloads [][]byte
		rngFrom       uint64
		rngLength     uint64
		expectRes     []byte
	}{
		{
			name:          "normal, same length",
			childPayloads: [][]byte{{0, 1, 2}, {3, 4, 3}, {2, 1, 0}},
			rngFrom:       4,
			rngLength:     3,
			expectRes:     []byte{4, 3, 2},
		},
		{
			name:          "normal, same length, range equals obj's bounds",
			childPayloads: [][]byte{{0, 1, 2}, {3, 4, 3}, {2, 1, 0}},
			rngFrom:       3,
			rngLength:     3,
			expectRes:     []byte{3, 4, 3},
		},
		{
			name:          "strange split, different length",
			childPayloads: [][]byte{{0, 1, 2, 4, 5, 6}, {5}, {4, 3, 2}, {1, 0}},
			rngFrom:       4,
			rngLength:     4,
			expectRes:     []byte{5, 6, 5, 4},
		},
		{
			name:          "strange split, obj with empty payload",
			childPayloads: [][]byte{{0, 1, 2}, {}, {}, {1, 0}},
			rngFrom:       2,
			rngLength:     2,
			expectRes:     []byte{2, 1},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			payloads := test.childPayloads

			children := make([]object.MeasuredObject, 0, len(payloads))
			for _, payload := range payloads {
				var child object.MeasuredObject
				child.SetObjectSize(uint32(len(payload)))

				children = append(children, child)
			}

			firstChild, firstChildOffset, lastChild, lastChildBound := requiredChildren(test.rngFrom, test.rngLength, children)

			// collect payload
			var res []byte
			for i := firstChild; i <= lastChild; i++ {
				var leftBound uint64
				var rightBound = uint64(children[i].ObjectSize())

				if i == firstChild {
					leftBound = firstChildOffset
				}

				if i == lastChild {
					rightBound = lastChildBound
				}

				if len(payloads[i]) > 0 {
					require.NotZero(t, rightBound-leftBound, "do not ask for empty payload ever")
				}

				payloadFromChild := payloads[i][leftBound:rightBound]
				res = append(res, payloadFromChild...)
			}

			require.Equal(t, test.expectRes, res)
		})
	}
}
