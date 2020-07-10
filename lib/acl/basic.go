package acl

import (
	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/internal"
)

type (
	// BasicChecker is an interface of the basic ACL control tool.
	BasicChecker interface {
		// Action returns true if request is allowed for this target.
		Action(uint32, object.RequestType, acl.Target) (bool, error)

		// Bearer returns true if bearer token is allowed for this request.
		Bearer(uint32, object.RequestType) (bool, error)

		// Extended returns true if extended ACL is allowed for this.
		Extended(uint32) bool

		// Sticky returns true if sticky bit is set.
		Sticky(uint32) bool
	}

	// BasicACLChecker performs basic ACL check.
	BasicACLChecker struct{}

	// MaskedBasicACLChecker performs all basic ACL checks, but applying
	// mask on ACL first. It is useful, when some bits must be always
	// set or unset.
	MaskedBasicACLChecker struct {
		BasicACLChecker

		andMask uint32
		orMask  uint32
	}

	nibble struct {
		value uint32
	}
)

const (
	errUnknownRequest = internal.Error("unknown request type")
	errUnknownTarget  = internal.Error("unknown target type")
)

const (
	aclFinalBit  = 0x10000000 // 29th bit
	aclStickyBit = 0x20000000 // 30th bit

	nibbleBBit = 0x1
	nibbleOBit = 0x2
	nibbleSBit = 0x4
	nibbleUBit = 0x8

	// DefaultAndFilter is a default AND mask of basic ACL value of container.
	DefaultAndFilter = 0xFFFFFFFF
)

var (
	nibbleOffset = map[object.RequestType]uint32{
		object.RequestGet:       0,
		object.RequestHead:      1 * 4,
		object.RequestPut:       2 * 4,
		object.RequestDelete:    3 * 4,
		object.RequestSearch:    4 * 4,
		object.RequestRange:     5 * 4,
		object.RequestRangeHash: 6 * 4,
	}
)

// Action returns true if request is allowed for target.
func (c *BasicACLChecker) Action(rule uint32, req object.RequestType, t acl.Target) (bool, error) {
	n, err := fetchNibble(rule, req)
	if err != nil {
		return false, err
	}

	switch t {
	case acl.Target_User:
		return n.U(), nil
	case acl.Target_System:
		return n.S(), nil
	case acl.Target_Others:
		return n.O(), nil
	default:
		return false, errUnknownTarget
	}
}

// Bearer returns true if bearer token is allowed to use for this request
// as source of extended ACL.
func (c *BasicACLChecker) Bearer(rule uint32, req object.RequestType) (bool, error) {
	n, err := fetchNibble(rule, req)
	if err != nil {
		return false, err
	}

	return n.B(), nil
}

// Extended returns true if extended ACL stored in the container are allowed
// to use.
func (c *BasicACLChecker) Extended(rule uint32) bool {
	return rule&aclFinalBit != aclFinalBit
}

// Sticky returns true if container is not allowed to store objects with
// owners different from request owner.
func (c *BasicACLChecker) Sticky(rule uint32) bool {
	return rule&aclStickyBit == aclStickyBit
}

func fetchNibble(rule uint32, req object.RequestType) (*nibble, error) {
	offset, ok := nibbleOffset[req]
	if !ok {
		return nil, errUnknownRequest
	}

	return &nibble{value: (rule >> offset) & 0xf}, nil
}

// B returns true if `Bearer` bit set in the nibble.
func (n *nibble) B() bool { return n.value&nibbleBBit == nibbleBBit }

// O returns true if `Others` bit set in the nibble.
func (n *nibble) O() bool { return n.value&nibbleOBit == nibbleOBit }

// S returns true if `System` bit set in the nibble.
func (n *nibble) S() bool { return n.value&nibbleSBit == nibbleSBit }

// U returns true if `User` bit set in the nibble.
func (n *nibble) U() bool { return n.value&nibbleUBit == nibbleUBit }

// NewMaskedBasicACLChecker returns BasicChecker that applies predefined
// bit mask on basic ACL value.
func NewMaskedBasicACLChecker(or, and uint32) BasicChecker {
	return MaskedBasicACLChecker{
		BasicACLChecker: BasicACLChecker{},
		andMask:         and,
		orMask:          or,
	}
}

// Action returns true if request is allowed for target.
func (c MaskedBasicACLChecker) Action(rule uint32, req object.RequestType, t acl.Target) (bool, error) {
	rule |= c.orMask
	rule &= c.andMask

	return c.BasicACLChecker.Action(rule, req, t)
}

// Bearer returns true if bearer token is allowed to use for this request
// as source of extended ACL.
func (c MaskedBasicACLChecker) Bearer(rule uint32, req object.RequestType) (bool, error) {
	rule |= c.orMask
	rule &= c.andMask

	return c.BasicACLChecker.Bearer(rule, req)
}

// Extended returns true if extended ACL stored in the container are allowed
// to use.
func (c MaskedBasicACLChecker) Extended(rule uint32) bool {
	rule |= c.orMask
	rule &= c.andMask

	return c.BasicACLChecker.Extended(rule)
}

// Sticky returns true if container is not allowed to store objects with
// owners different from request owner.
func (c MaskedBasicACLChecker) Sticky(rule uint32) bool {
	rule |= c.orMask
	rule &= c.andMask

	return c.BasicACLChecker.Sticky(rule)
}
