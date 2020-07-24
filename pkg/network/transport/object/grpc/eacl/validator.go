package eacl

import (
	"bytes"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// RequestInfo represents the information
// about the request.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended.RequestInfo.
type RequestInfo = extended.RequestInfo

// Action represents action on the request.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended.Action.
type Action = eacl.Action

// Storage represents the eACL table storage.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage.Storage.
type Storage = storage.Storage

// Target represents authorization group.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.Target.
type Target = eacl.Target

// Table represents extended ACL rule table.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.ExtendedACLTable.
type Table = eacl.Table

// HeaderFilter represents the header filter.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.HeaderFilter.
type HeaderFilter = eacl.HeaderFilter

// Header represents the string
// key-value header.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended.Header.
type Header = extended.Header

// MatchType represents value match type.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.MatchType.
type MatchType = eacl.MatchType

// Validator is a tool that calculates
// the action on a request according
// to the extended ACL rule table.
//
// Validator receives eACL table from
// the eACL storage.
type Validator struct {
	logger *logger.Logger // logging component

	storage Storage // eACL table storage
}

// NewValidator creates and initializes a new Validator using arguments.
//
// Returns an error if some of the arguments is nil.
//
// Using the Validator that has been created with new(Validator)
// expression (or just declaring a Validator variable) is unsafe
// and can lead to panic.
func NewValidator(st Storage, lg *logger.Logger) (*Validator, error) {
	switch {
	case st == nil:
		return nil, storage.ErrNilStorage
	case lg == nil:
		return nil, logger.ErrNilLogger
	}

	return &Validator{
		logger:  lg,
		storage: st,
	}, nil
}

// CalculateAction calculates action on the request according
// to its information.
//
// The action is calculated according to the application of
// eACL table of rules to the request.
//
// If request info argument is nil, eacl.ActionUnknown is
// returned immediately.
//
// If the eACL table is not available at the time of the call,
// eacl.ActionUnknown is returned.
//
// If no matching table entry is found, ActionAllow is returned.
func (v *Validator) CalculateAction(info RequestInfo) Action {
	if info == nil {
		return eacl.ActionUnknown
	}

	// get container identifier from request
	cid := info.CID()

	// get eACL table by container ID
	table, err := v.storage.GetEACL(cid)
	if err != nil {
		v.logger.Error("could not get eACL table",
			zap.Stringer("cid", cid),
			zap.String("error", err.Error()),
		)

		return eacl.ActionUnknown
	}

	return tableAction(info, table)
}

// calculates action on the request based on the eACL rules.
func tableAction(info RequestInfo, table Table) Action {
	requestOpType := info.OperationType()

	for _, record := range table.Records() {
		// check type of operation
		if record.OperationType() != requestOpType {
			continue
		}

		// check target
		if !targetMatches(info, record.TargetList()) {
			continue
		}

		// check headers
		switch val := matchFilters(info, record.HeaderFilters()); {
		case val < 0:
			// headers of some type could not be composed => allow
			return eacl.ActionAllow
		case val == 0:
			return record.Action()
		}
	}

	return eacl.ActionAllow
}

// returns:
//  - positive value if no matching header is found for at least one filter;
//  - zero if at least one suitable header is found for all filters;
//  - negative value if the headers of at least one filter cannot be obtained.
func matchFilters(info extended.TypedHeaderSource, filters []HeaderFilter) int {
	matched := 0

	for _, filter := range filters {
		// prevent NPE
		if filter == nil {
			continue
		}

		headers, ok := info.HeadersOfType(filter.HeaderType())
		if !ok {
			return -1
		}

		// get headers of filtering type
		for _, header := range headers {
			// prevent NPE
			if header == nil {
				continue
			}

			// check header name
			if header.Name() != filter.Name() {
				continue
			}

			// get match function
			matchFn, ok := mMatchFns[filter.MatchType()]
			if !ok {
				continue
			}

			// check match
			if !matchFn(header, filter) {
				continue
			}

			// increment match counter
			matched++

			break
		}
	}

	return len(filters) - matched
}

// returns true if one of ExtendedACLTarget has
// suitable target OR suitable public key.
func targetMatches(req RequestInfo, groups []Target) bool {
	requestKey := req.Key()

	for _, target := range groups {
		recordGroup := target.Group()

		// check public key match
		for _, key := range target.KeyList() {
			if bytes.Equal(key, requestKey) {
				return true
			}
		}

		// check target group match
		if req.Group() == recordGroup {
			return true
		}
	}

	return false
}

// Maps MatchType to corresponding function.
// 1st argument of function - header value, 2nd - header filter.
var mMatchFns = map[MatchType]func(Header, Header) bool{
	eacl.StringEqual: func(header Header, filter Header) bool {
		return header.Value() == filter.Value()
	},

	eacl.StringNotEqual: func(header Header, filter Header) bool {
		return header.Value() != filter.Value()
	},
}
