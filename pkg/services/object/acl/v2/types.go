package v2

import (
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// ACLChecker is an interface that must provide
// ACL related checks.
type ACLChecker interface {
	// CheckBasicACL must return true only if request
	// passes basic ACL validation.
	CheckBasicACL(RequestInfo) bool
	// CheckEACL must return non-nil error if request
	// doesn't pass extended ACL validation.
	CheckEACL(any, RequestInfo) error
	// StickyBitCheck must return true only if sticky bit
	// is disabled or enabled but request contains correct
	// owner field.
	StickyBitCheck(RequestInfo, user.ID) bool
}

// InnerRingFetcher is an interface that must provide
// Inner Ring information.
type InnerRingFetcher interface {
	// InnerRingKeys must return list of public keys of
	// the actual inner ring.
	InnerRingKeys() ([][]byte, error)
}
