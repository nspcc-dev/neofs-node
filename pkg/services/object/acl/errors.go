package acl

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
)

// Various generic errors.
var (
	// errInvalidSignature is returned when digital signature is considered invalid.
	errInvalidSignature = errors.New("invalid signature")
	// errMissingObjectHeader is returned when object headers is missing.
	errMissingObjectHeader = errors.New("missing object header")
	// errMissingObjectOwner is returned when object owner is missing.
	errMissingObjectOwner = errors.New("missing object owner")
	// errObjectOwnerAuth is returned when object owner's authorization failed.
	errObjectOwnerAuth = errors.New("object owner authorization failed")
)

// invalidAtEpochError is returned when some entity is invalid at particular
// epoch.
type invalidAtEpochError struct {
	epoch uint64
}

// newInvalidAtEpochError constructs invalidAtEpochError with the given epoch.
func newInvalidAtEpochError(epoch uint64) invalidAtEpochError {
	return invalidAtEpochError{
		epoch: epoch,
	}
}

func (x invalidAtEpochError) Error() string {
	return fmt.Sprintf("invalid at epoch #%d", x.epoch)
}

// ErrNotEnoughData is returned when the current data is insufficient for
// verification.
var ErrNotEnoughData = errors.New("not enough data")

// InvalidRequestError is an error returned when request data is considered
// invalid for some reason. The reason is wrapped compatible with [errors]
// package functionality.
type InvalidRequestError struct {
	cause error
}

// newInvalidRequestError constructs InvalidRequestError with the given cause.
func newInvalidRequestError(cause error) InvalidRequestError {
	return InvalidRequestError{
		cause: cause,
	}
}

func (x InvalidRequestError) Error() string {
	return fmt.Sprintf("invalid request: %s", x.cause)
}

// Unwrap unwraps the InvalidRequestError cause.
func (x InvalidRequestError) Unwrap() error {
	return x.cause
}

// AccessDeniedError is returned when access is denied for some reason. The
// reason is wrapped compatible with [errors] package functionality.
type AccessDeniedError struct {
	cause error
}

// newAccessDeniedError constructs AccessDeniedError with the given cause.
func newAccessDeniedError(cause error) AccessDeniedError {
	return AccessDeniedError{
		cause: cause,
	}
}

func (x AccessDeniedError) Error() string {
	return fmt.Sprintf("access denied: %s", x.cause)
}

// Unwrap unwraps the AccessDeniedError cause.
func (x AccessDeniedError) Unwrap() error {
	return x.cause
}

// basicAccessRuleError is triggered by some basic access rule. The reason is
// wrapped compatible with [errors] package functionality.
type basicAccessRuleError struct {
	cause error
}

// newBasicRuleError constructs basicAccessRuleError with the given cause.
func newBasicRuleError(cause error) basicAccessRuleError {
	return basicAccessRuleError{
		cause: cause,
	}
}

func (x basicAccessRuleError) Error() string {
	return fmt.Sprintf("basic access rule triggered: %s", x.cause)
}

// Unwrap unwraps the basicAccessRuleError cause.
func (x basicAccessRuleError) Unwrap() error {
	return x.cause
}

// forbiddenClientRoleError is returned on forbidden client role.
type forbiddenClientRoleError acl.Role

// newForbiddenClientRoleError constructs forbiddenClientRoleError for the given
// client role.
func newForbiddenClientRoleError(role acl.Role) forbiddenClientRoleError {
	return forbiddenClientRoleError(role)
}

func (x forbiddenClientRoleError) Error() string {
	return fmt.Sprintf("forbidden client role %s", acl.Role(x))
}

// stickyAccessRuleError is triggered by the sticky access rule. The reason is
// wrapped compatible with [errors] package functionality.
type stickyAccessRuleError struct {
	cause error
}

// newStickyAccessRuleError constructs stickyAccessRuleError with the given cause.
func newStickyAccessRuleError(cause error) stickyAccessRuleError {
	return stickyAccessRuleError{
		cause: cause,
	}
}

func (x stickyAccessRuleError) Error() string {
	return fmt.Sprintf("basic access rule triggered: %s", x.cause)
}

// Unwrap unwraps the basicAccessRuleError cause.
func (x stickyAccessRuleError) Unwrap() error {
	return x.cause
}

// extendedAccessRuleError is triggered by some extended access rule.
type extendedAccessRuleError struct {
	index int
}

// newExtendedRuleError constructs extendedAccessRuleError for the given list
// index.
func newExtendedRuleError(index int) extendedAccessRuleError {
	return extendedAccessRuleError{
		index: index,
	}
}

func (x extendedAccessRuleError) Error() string {
	return fmt.Sprintf("access rule #%d triggered", x.index)
}

// invalidBearerTokenError is returned when a bearer token is considered invalid
// for some reason. The reason is wrapped compatible with [errors] package
// functionality.
type invalidBearerTokenError struct {
	cause error
}

// Various bearer errors.
var (
	// errBearerIssuerAuth is returned when bearer issuer authorization failed.
	errBearerIssuerAuth = errors.New("bearer token is not issued by the owner of the requested container")
	// errBearerClientAuth is returned when bearer client authorization failed.
	errBearerClientAuth = errors.New("bearer token is not subject for the client")
	// errBearerContainerMismatch is returned when bearer context does not apply to
	// the requested container.
	errBearerContainerMismatch = errors.New("bearer token does not target the requested container")
)

// newInvalidBearerTokenError constructs invalidBearerTokenError with the given cause.
func newInvalidBearerTokenError(cause error) invalidBearerTokenError {
	return invalidBearerTokenError{
		cause: cause,
	}
}

func (x invalidBearerTokenError) Error() string {
	return fmt.Sprintf("invalid bearer token: %s", x.cause)
}

// Unwrap unwraps the invalidBearerTokenError cause.
func (x invalidBearerTokenError) Unwrap() error {
	return x.cause
}

// invalidSessionTokenError is returned when a session token is considered
// invalid for some reason. The reason is wrapped compatible with [errors]
// package functionality.
type invalidSessionTokenError struct {
	cause error
}

// Various session errors.
var (
	// errSessionIssuerAuth is returned when session issuer authorization failed.
	errSessionIssuerAuth = errors.New("session token is signed by the key not bound to the issuer")
	// errSessionClientAuth is returned when session client authorization failed.
	errSessionClientAuth = errors.New("session is not subject for the client")
	// errSessionOpMismatch is returned when session context does not apply to the
	// requested operation.
	errSessionOpMismatch = errors.New("session does not target the requested container")
	// errSessionContainerMismatch is returned when session context does not apply
	// to the requested container.
	errSessionContainerMismatch = errors.New("session does not target the requested container")
	// errSessionObjectMismatch is returned when session context does not apply to
	// the requested object.
	errSessionObjectMismatch = errors.New("session does not target the requested object")
)

// newInvalidSessionTokenError constructs invalidSessionTokenError with the given cause.
func newInvalidSessionTokenError(cause error) invalidSessionTokenError {
	return invalidSessionTokenError{
		cause: cause,
	}
}

func (x invalidSessionTokenError) Error() string {
	return fmt.Sprintf("invalid session token: %s", x.cause)
}

// Unwrap unwraps the invalidSessionTokenError cause.
func (x invalidSessionTokenError) Unwrap() error {
	return x.cause
}
