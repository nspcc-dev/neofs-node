package container

import "github.com/nspcc-dev/neo-go/pkg/network/payload"

// AnnounceLoad structure of container.AnnounceLoad notification from morph chain.
type AnnounceLoad struct {
	epoch uint64
	cnrID []byte
	val   uint64
	key   []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (AnnounceLoad) MorphEvent() {}

// Epoch returns epoch when estimation of the container data size was calculated.
func (al AnnounceLoad) Epoch() uint64 { return al.epoch }

// ContainerID returns container ID for which the amount of data is estimated.
func (al AnnounceLoad) ContainerID() []byte { return al.cnrID }

// Value returns estimated amount of data (in bytes) in the specified container.
func (al AnnounceLoad) Value() uint64 { return al.val }

// Key returns a public key of the reporter.
func (al AnnounceLoad) Key() []byte { return al.key }

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (al AnnounceLoad) NotaryRequest() *payload.P2PNotaryRequest {
	return al.notaryRequest
}
