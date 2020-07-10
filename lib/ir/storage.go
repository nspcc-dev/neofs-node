package ir

import (
	"bytes"

	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
)

// Storage is an interface of the storage of info about NeoFS IR.
type Storage interface {
	GetIRInfo(GetInfoParams) (*GetInfoResult, error)
}

// GetInfoParams is a structure that groups the parameters
// for IR info receiving operation.
type GetInfoParams struct {
}

// GetInfoResult is a structure that groups
// values returned by IR info receiving operation.
type GetInfoResult struct {
	info Info
}

// ErrNilStorage is returned by functions that expect
// a non-nil Storage, but received nil.
const ErrNilStorage = internal.Error("inner ring storage is nil")

// SetInfo is an IR info setter.
func (s *GetInfoResult) SetInfo(v Info) {
	s.info = v
}

// Info is an IR info getter.
func (s GetInfoResult) Info() Info {
	return s.info
}

// BinaryKeyList returns the list of binary public key of IR nodes.
//
// If passed Storage is nil, ErrNilStorage returns.
func BinaryKeyList(storage Storage) ([][]byte, error) {
	if storage == nil {
		return nil, ErrNilStorage
	}

	// get IR info
	getRes, err := storage.GetIRInfo(GetInfoParams{})
	if err != nil {
		return nil, errors.Wrap(
			err,
			"could not get information about IR",
		)
	}

	nodes := getRes.Info().Nodes()

	keys := make([][]byte, 0, len(nodes))

	for i := range nodes {
		keys = append(keys, nodes[i].Key())
	}

	return keys, nil
}

// IsInnerRingKey checks if the passed argument is the
// key of one of IR nodes.
//
// Uses BinaryKeyList function to receive the key list of IR nodes internally.
//
// If passed key slice is empty, crypto.ErrEmptyPublicKey returns immediately.
func IsInnerRingKey(storage Storage, key []byte) (bool, error) {
	// check key emptiness
	// TODO: summarize the void check to a full IR key-format check.
	if len(key) == 0 {
		return false, crypto.ErrEmptyPublicKey
	}

	irKeys, err := BinaryKeyList(storage)
	if err != nil {
		return false, err
	}

	for i := range irKeys {
		if bytes.Equal(irKeys[i], key) {
			return true, nil
		}
	}

	return false, nil
}
