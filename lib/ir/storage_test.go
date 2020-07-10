package ir

import (
	"testing"

	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testInfoReceiver struct {
	keys [][]byte

	err error
}

func (s testInfoReceiver) GetIRInfo(GetInfoParams) (*GetInfoResult, error) {
	if s.err != nil {
		return nil, s.err
	}

	nodes := make([]Node, 0, len(s.keys))

	for i := range s.keys {
		node := Node{}
		node.SetKey(s.keys[i])

		nodes = append(nodes, node)
	}

	info := Info{}
	info.SetNodes(nodes)

	res := new(GetInfoResult)
	res.SetInfo(info)

	return res, nil
}

func (s *testInfoReceiver) addKey(key []byte) {
	s.keys = append(s.keys, key)
}

func TestGetInfoResult(t *testing.T) {
	s := GetInfoResult{}

	info := Info{}

	n := Node{}
	n.SetKey([]byte{1, 2, 3})

	info.SetNodes([]Node{
		n,
	})

	s.SetInfo(info)

	require.Equal(t, info, s.Info())
}

func TestIsInnerRingKey(t *testing.T) {
	var (
		res bool
		err error
		s   = new(testInfoReceiver)
	)

	// empty public key
	res, err = IsInnerRingKey(nil, nil)
	require.EqualError(t, err, crypto.ErrEmptyPublicKey.Error())

	key := []byte{1, 2, 3}

	// nil Storage
	res, err = IsInnerRingKey(nil, key)
	require.EqualError(t, err, ErrNilStorage.Error())

	// force Storage to return an error
	s.err = errors.New("some error")

	// Storage error
	res, err = IsInnerRingKey(s, key)
	require.EqualError(t, errors.Cause(err), s.err.Error())

	// reset Storage error
	s.err = nil

	// IR keys don't contain key
	s.addKey(append(key, 1))

	res, err = IsInnerRingKey(s, key)
	require.NoError(t, err)
	require.False(t, res)

	// IR keys contain key
	s.addKey(key)

	res, err = IsInnerRingKey(s, key)
	require.NoError(t, err)
	require.True(t, res)
}
