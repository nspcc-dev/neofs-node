package acl

import (
	"github.com/nspcc-dev/neofs-api-go/acl"
)

// Maps MatchType to corresponding function.
// 1st argument of function - header value, 2nd - header filter.
var mMatchFns = map[acl.MatchType]func(acl.Header, acl.Header) bool{
	acl.StringEqual: stringEqual,

	acl.StringNotEqual: stringNotEqual,
}

const (
	mResUndefined = iota
	mResMatch
	mResMismatch
)

// MatchFilters checks if passed source carry at least one header that satisfies passed filters.
//
// Nil header does not satisfy any filter. Any header does not satisfy nil filter.
//
// Returns mResMismatch if passed TypedHeaderSource is nil.
// Returns mResMatch if passed filters are empty.
//
// If headers for some of the HeaderType could not be composed, mResUndefined returns.
func MatchFilters(src TypedHeaderSource, filters []acl.HeaderFilter) int {
	if src == nil {
		return mResMismatch
	} else if len(filters) == 0 {
		return mResMatch
	}

	matched := 0

	for _, filter := range filters {
		// prevent NPE
		if filter == nil {
			continue
		}

		headers, ok := src.HeadersOfType(filter.HeaderType())
		if !ok {
			return mResUndefined
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

	res := mResMismatch

	if matched >= len(filters) {
		res = mResMatch
	}

	return res
}

func stringEqual(header, filter acl.Header) bool {
	return header.Value() == filter.Value()
}

func stringNotEqual(header, filter acl.Header) bool {
	return header.Value() != filter.Value()
}
