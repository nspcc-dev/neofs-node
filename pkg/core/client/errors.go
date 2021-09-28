package client

import (
	"errors"
)

// ErrWrongPublicKey is returned when the client's response is signed with a key different
// from the one declared in the network map.
var ErrWrongPublicKey = errors.New("public key is different from the key in the network map")
