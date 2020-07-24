package node

import (
	"errors"
)

// Info represents the information
// about NeoFS storage node.
type Info struct {
	address string // net address

	key []byte // public key

	opts []string // options

	status Status // status bits
}

// ErrNilInfo is returned by functions that expect
// a non-nil Info pointer, but received nil.
var ErrNilInfo = errors.New("node info is nil")

// Address returns node network address.
//
// Address format is dictated by
// application architecture.
func (i Info) Address() string {
	return i.address
}

// SetAddress sets node network address.
func (i *Info) SetAddress(v string) {
	i.address = v
}

// Status returns the node status.
func (i Info) Status() Status {
	return i.status
}

// SetStatus sets the node status.
func (i *Info) SetStatus(v Status) {
	i.status = v
}

// PublicKey returns node public key in
// a binary format.
//
// Changing the result is unsafe and
// affects the node info. In order to
// prevent state mutations, use
// CopyPublicKey.
//
// Key format is dictated by
// application architecture.
func (i Info) PublicKey() []byte {
	return i.key
}

// CopyPublicKey returns the copy of
// node public key.
//
// Changing the result is safe and
// does not affect the node info.
func CopyPublicKey(i Info) []byte {
	res := make([]byte, len(i.key))

	copy(res, i.key)

	return res
}

// SetPublicKey sets node public key
// in a binary format.
//
// Subsequent changing the source slice
// is unsafe and affects node info.
// In order to prevent state mutations,
// use SetPublicKeyCopy.
func (i *Info) SetPublicKey(v []byte) {
	i.key = v
}

// SetPublicKeyCopy copies public key and
// sets the copy as node public key.
//
// Subsequent changing the source slice
// is safe and does not affect node info.
//
// Returns ErrNilInfo on nil node info.
func SetPublicKeyCopy(i *Info, key []byte) error {
	if i == nil {
		return ErrNilInfo
	}

	i.key = make([]byte, len(key))

	copy(i.key, key)

	return nil
}

// Options returns node option list.
//
// Changing the result is unsafe and
// affects the node info. In order to
// prevent state mutations, use
// CopyOptions.
//
// Option format is dictated by
// application architecture.
func (i Info) Options() []string {
	return i.opts
}

// CopyOptions returns the copy of
// node options list.
//
// Changing the result is safe and
// does not affect the node info.
func CopyOptions(i Info) []string {
	res := make([]string, len(i.opts))

	copy(res, i.opts)

	return res
}

// SetOptions sets node option list.
//
// Subsequent changing the source slice
// is unsafe and affects node info.
// In order to prevent state mutations,
// use SetOptionsCopy.
func (i *Info) SetOptions(v []string) {
	i.opts = v
}

// SetOptionsCopy copies option list and sets
// the copy as node options list.
//
// Subsequent changing the source slice
// is safe and does not affect node info.
//
// SetOptionsCopy does nothing if Info is nil.
func SetOptionsCopy(i *Info, opts []string) {
	if i == nil {
		return
	}

	i.opts = make([]string, len(opts))

	copy(i.opts, opts)
}
