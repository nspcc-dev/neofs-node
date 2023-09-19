package v2acl

import (
	"errors"
	"fmt"
)

// Various generic errors.
var (
	// errMissingRequestBody is returned when request body field is missing.
	errMissingRequestBody = errors.New("missing request body")
	// errMissingRequestVerificationHeader is returned when request verification header is missing.
	errMissingRequestVerificationHeader = errors.New("missing request verification header")
	// errMultipleStreamInitializers is returned when initial message of the stream
	// is repeated while must be single.
	errMultipleStreamInitializers = errors.New("multiple stream initializers")
	// errMissingBodySignature is returned when request body is missing.
	errMissingBodySignature = errors.New("missing body signature")
	// errMissingPublicKey is returned when public key is missing.
	errMissingPublicKey = errors.New("missing public key")
	// errMissingObjectAddress is returned when object address is missing.
	errMissingObjectAddress = errors.New("missing object address")
	// errMissingContainerID is returned when container ID is missing.
	errMissingContainerID = errors.New("missing container ID")
	// errMissingObjectID is returned when object ID is missing.
	errMissingObjectID = errors.New("missing object ID")
	// errMissingObjectHeader is returned when object headers is missing.
	errMissingObjectHeader = errors.New("missing object header")
)

// newInvalidRequestError wraps the error caused invalid request error
// compatible with [errors] package functionality.
func newInvalidRequestError(cause error) error {
	return fmt.Errorf("invalid request: %w", cause)
}

// newInvalidRequestBodyError wraps the error caused invalid request body error
// compatible with [errors] package functionality.
func newInvalidRequestBodyError(cause error) error {
	return fmt.Errorf("invalid request body: %w", cause)
}

// newInvalidMetaHeaderError wraps the error caused invalid meta header error
// compatible with [errors] package functionality.
func newInvalidMetaHeaderError(cause error) error {
	return fmt.Errorf("invalid request meta header: %w", cause)
}

// newInvalidVerificationHeaderError wraps the error caused invalid verification
// header error compatible with [errors] package functionality.
func newInvalidVerificationHeaderError(cause error) error {
	return fmt.Errorf("invalid request verification header: %w", cause)
}

// newInvalidObjectAddressError wraps the error caused invalid object address
// error compatible with [errors] package functionality.
func newInvalidObjectAddressError(cause error) error {
	return fmt.Errorf("invalid address: %w", cause)
}

// newInvalidContainerIDError wraps the error caused invalid container ID error
// compatible with [errors] package functionality.
func newInvalidContainerIDError(cause error) error {
	return fmt.Errorf("invalid container ID: %w", cause)
}

// newInvalidObjectIDError wraps the error caused invalid object ID error
// compatible with [errors] package functionality.
func newInvalidObjectIDError(cause error) error {
	return fmt.Errorf("invalid object ID: %w", cause)
}
