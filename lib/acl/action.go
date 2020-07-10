package acl

import (
	"bytes"

	"github.com/nspcc-dev/neofs-api-go/acl"
)

// RequestInfo is an interface of request information needed for extended ACL check.
type RequestInfo interface {
	TypedHeaderSource

	// Must return the binary representation of request initiator's key.
	Key() []byte

	// Must return true if request corresponds to operation type.
	TypeOf(acl.OperationType) bool

	// Must return true if request has passed target.
	TargetOf(acl.Target) bool
}

// ExtendedACLChecker is an interface of extended ACL checking tool.
type ExtendedACLChecker interface {
	// Must return an action according to the results of applying the ACL table rules to request.
	//
	// Must return ActionUndefined if it is unable to explicitly calculate the action.
	Action(acl.ExtendedACLTable, RequestInfo) acl.ExtendedACLAction
}

type extendedACLChecker struct{}

// NewExtendedACLChecker creates a new extended ACL checking tool and returns ExtendedACLChecker interface.
func NewExtendedACLChecker() ExtendedACLChecker {
	return new(extendedACLChecker)
}

// Action returns an action for passed request based on information about it and ACL table.
//
// Returns action of the first suitable table record, or ActionUndefined in the absence thereof.
//
// If passed ExtendedACLTable is nil, ActionUndefined returns.
// If passed RequestInfo is nil, ActionUndefined returns.
func (s extendedACLChecker) Action(table acl.ExtendedACLTable, req RequestInfo) acl.ExtendedACLAction {
	if table == nil {
		return acl.ActionUndefined
	} else if req == nil {
		return acl.ActionUndefined
	}

	for _, record := range table.Records() {
		// check type of operation
		if !req.TypeOf(record.OperationType()) {
			continue
		}

		// check target
		if !targetMatches(req, record.TargetList()) {
			continue
		}

		// check headers
		switch MatchFilters(req, record.HeaderFilters()) {
		case mResUndefined:
			// headers of some type could not be composed => allow
			return acl.ActionAllow
		case mResMatch:
			return record.Action()
		}
	}

	return acl.ActionAllow
}

// returns true if one of ExtendedACLTarget has suitable target OR suitable public key.
func targetMatches(req RequestInfo, list []acl.ExtendedACLTarget) bool {
	rKey := req.Key()

	for _, target := range list {
		// check public key match
		for _, key := range target.KeyList() {
			if bytes.Equal(key, rKey) {
				return true
			}
		}

		// check target group match
		if req.TargetOf(target.Target()) {
			return true
		}
	}

	return false
}
