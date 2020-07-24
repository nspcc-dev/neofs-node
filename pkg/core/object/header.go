package object

import (
	"errors"
)

// Header represents NeoFS object header.
type Header struct {
	// SystemHeader is an obligatory part of any object header.
	// It is used to set the identity and basic parameters of
	// the object.
	//
	// Header must inherit all the methods of SystemHeader,
	// so the SystemHeader is embedded in Header.
	SystemHeader

	extendedHeaders []ExtendedHeader // extended headers
}

// ErrNilHeader is returned by functions that expect
// a non-nil Header pointer, but received nil.
var ErrNilHeader = errors.New("object header is nil")

// ExtendedHeaders returns the extended headers of header.
//
// Changing the result is unsafe and affects the header.
// In order to prevent state mutations, use CopyExtendedHeaders.
func (h *Header) ExtendedHeaders() []ExtendedHeader {
	return h.extendedHeaders
}

// CopyExtendedHeaders returns the copy of extended headers.
//
// Changing the result is safe and does not affect the header.
//
// Returns nil if header is nil.
func CopyExtendedHeaders(h *Header) []ExtendedHeader {
	if h == nil {
		return nil
	}

	res := make([]ExtendedHeader, len(h.extendedHeaders))

	copy(res, h.extendedHeaders)

	return res
}

// SetExtendedHeaders sets the extended headers of the header.
//
// Subsequent changing the source slice is unsafe and affects
// the header. In order to prevent state mutations, use
// SetExtendedHeadersCopy.
func (h *Header) SetExtendedHeaders(v []ExtendedHeader) {
	h.extendedHeaders = v
}

// SetExtendedHeadersCopy copies extended headers and sets the copy
// as the object extended headers.
//
// Subsequent changing the source slice is safe and does not affect
// the header.
//
// SetExtendedHeadersCopy does nothing if Header is nil.
func SetExtendedHeadersCopy(h *Header, hs []ExtendedHeader) {
	if h == nil {
		return
	}

	h.extendedHeaders = make([]ExtendedHeader, len(hs))

	copy(h.extendedHeaders, hs)
}
