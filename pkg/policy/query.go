package policy

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
)

var (
	ErrInvalidNumber   = errors.New("policy: expected positive integer")
	ErrUnknownClause   = errors.New("policy: unknown clause")
	ErrUnknownOp       = errors.New("policy: unknown operation")
	ErrUnknownFilter   = errors.New("policy: filter not found")
	ErrUnknownSelector = errors.New("policy: selector not found")
)

func parse(s string) (*query, error) {
	q := new(query)
	err := parser.Parse(strings.NewReader(s), q)
	if err != nil {
		return nil, err
	}
	return q, nil
}

// Parse parses s into a placement policy.
func Parse(s string) (*netmap.PlacementPolicy, error) {
	q, err := parse(s)
	if err != nil {
		return nil, err
	}

	seenFilters := map[string]bool{}
	fs := make([]*netmap.Filter, 0, len(q.Filters))
	for _, qf := range q.Filters {
		f, err := filterFromOrChain(qf.Value, seenFilters)
		if err != nil {
			return nil, err
		}
		f.SetName(qf.Name)
		fs = append(fs, f)
		seenFilters[qf.Name] = true
	}

	seenSelectors := map[string]bool{}
	ss := make([]*netmap.Selector, 0, len(q.Selectors))
	for _, qs := range q.Selectors {
		if qs.Filter != netmap.MainFilterName && !seenFilters[qs.Filter] {
			return nil, fmt.Errorf("%w: '%s'", ErrUnknownFilter, qs.Filter)
		}
		s := netmap.NewSelector()
		switch len(qs.Bucket) {
		case 1: // only bucket
			s.SetAttribute(qs.Bucket[0])
		case 2: // clause + bucket
			s.SetClause(clauseFromString(qs.Bucket[0]))
			s.SetAttribute(qs.Bucket[1])
		}
		s.SetName(qs.Name)
		seenSelectors[qs.Name] = true
		s.SetFilter(qs.Filter)
		if qs.Count == 0 {
			return nil, fmt.Errorf("%w: SELECT", ErrInvalidNumber)
		}
		s.SetCount(qs.Count)
		ss = append(ss, s)
	}

	rs := make([]*netmap.Replica, 0, len(q.Replicas))
	for _, qr := range q.Replicas {
		r := netmap.NewReplica()
		if qr.Selector != "" {
			if !seenSelectors[qr.Selector] {
				return nil, fmt.Errorf("%w: '%s'", ErrUnknownSelector, qr.Selector)
			}
			r.SetSelector(qr.Selector)
		}
		if qr.Count == 0 {
			return nil, fmt.Errorf("%w: REP", ErrInvalidNumber)
		}
		r.SetCount(uint32(qr.Count))
		rs = append(rs, r)
	}

	p := new(netmap.PlacementPolicy)
	p.SetFilters(fs...)
	p.SetSelectors(ss...)
	p.SetReplicas(rs...)
	p.SetContainerBackupFactor(q.CBF)

	return p, nil
}

func clauseFromString(s string) netmap.Clause {
	switch strings.ToUpper(s) {
	case "SAME":
		return netmap.ClauseSame
	case "DISTINCT":
		return netmap.ClauseDistinct
	default:
		return 0
	}
}

func filterFromOrChain(expr *orChain, seen map[string]bool) (*netmap.Filter, error) {
	var fs []*netmap.Filter
	for _, ac := range expr.Clauses {
		f, err := filterFromAndChain(ac, seen)
		if err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	if len(fs) == 1 {
		return fs[0], nil
	}

	f := netmap.NewFilter()
	f.SetOperation(netmap.OpOR)
	f.SetInnerFilters(fs...)
	return f, nil
}

func filterFromAndChain(expr *andChain, seen map[string]bool) (*netmap.Filter, error) {
	var fs []*netmap.Filter
	for _, fe := range expr.Clauses {
		var f *netmap.Filter
		var err error
		if fe.Expr != nil {
			f, err = filterFromSimpleExpr(fe.Expr, seen)
		} else {
			f = netmap.NewFilter()
			f.SetName(fe.Reference)
		}
		if err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	if len(fs) == 1 {
		return fs[0], nil
	}

	f := netmap.NewFilter()
	f.SetOperation(netmap.OpAND)
	f.SetInnerFilters(fs...)
	return f, nil
}

func filterFromSimpleExpr(se *simpleExpr, seen map[string]bool) (*netmap.Filter, error) {
	f := netmap.NewFilter()
	f.SetKey(se.Key)
	switch se.Op {
	case "EQ":
		f.SetOperation(netmap.OpEQ)
	case "NE":
		f.SetOperation(netmap.OpNE)
	case "GE":
		f.SetOperation(netmap.OpGE)
	case "GT":
		f.SetOperation(netmap.OpGT)
	case "LE":
		f.SetOperation(netmap.OpLE)
	case "LT":
		f.SetOperation(netmap.OpLT)
	default:
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownOp, se.Op)
	}
	f.SetValue(se.Value)
	return f, nil
}
