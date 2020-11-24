package eacl

import (
	"bytes"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Validator is a tool that calculates
// the action on a request according
// to the extended ACL rule table.
type Validator struct {
	*cfg
}

// Option represents Validator option.
type Option func(*cfg)

type cfg struct {
	logger *logger.Logger

	storage Storage
}

func defaultCfg() *cfg {
	return &cfg{
		logger: zap.L(),
	}
}

// NewValidator creates and initializes a new Validator using options.
func NewValidator(opts ...Option) *Validator {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &Validator{
		cfg: cfg,
	}
}

// CalculateAction calculates action on the request according
// to its information represented in ValidationUnit.
//
// The action is calculated according to the application of
// eACL table of rules to the request.
//
// If the eACL table is not available at the time of the call,
// eacl.ActionUnknown is returned.
//
// If no matching table entry is found, ActionAllow is returned.
func (v *Validator) CalculateAction(unit *ValidationUnit) eacl.Action {
	var (
		err   error
		table *eacl.Table
	)

	if unit.bearer != nil {
		table = eacl.NewTableFromV2(unit.bearer.GetBody().GetEACL())
	} else {
		// get eACL table by container ID
		table, err = v.storage.GetEACL(unit.cid)
		if err != nil {
			if errors.Is(err, container.ErrEACLNotFound) {
				return eacl.ActionAllow
			}

			v.logger.Error("could not get eACL table",
				zap.String("error", err.Error()),
			)

			return eacl.ActionUnknown
		}
	}

	return tableAction(unit, table)
}

// calculates action on the request based on the eACL rules.
func tableAction(unit *ValidationUnit, table *eacl.Table) eacl.Action {
	for _, record := range table.Records() {
		// check type of operation
		if record.Operation() != unit.op {
			continue
		}

		// check target
		if !targetMatches(unit, record) {
			continue
		}

		// check headers
		switch val := matchFilters(unit.hdrSrc, record.Filters()); {
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
func matchFilters(hdrSrc TypedHeaderSource, filters []*eacl.Filter) int {
	matched := 0

	for _, filter := range filters {
		headers, ok := hdrSrc.HeadersOfType(filter.From())
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
			if header.Key() != filter.Key() {
				continue
			}

			// get match function
			matchFn, ok := mMatchFns[filter.Matcher()]
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
func targetMatches(unit *ValidationUnit, record *eacl.Record) bool {
	for _, target := range record.Targets() {
		// check public key match
		for _, key := range target.Keys() {
			if bytes.Equal(crypto.MarshalPublicKey(&key), unit.key) {
				return true
			}
		}

		// check target group match
		if unit.role == target.Role() {
			return true
		}
	}

	return false
}

// Maps match type to corresponding function.
var mMatchFns = map[eacl.Match]func(Header, *eacl.Filter) bool{
	eacl.MatchStringEqual: func(header Header, filter *eacl.Filter) bool {
		return header.Value() == filter.Value()
	},

	eacl.MatchStringNotEqual: func(header Header, filter *eacl.Filter) bool {
		return header.Value() != filter.Value()
	},
}
