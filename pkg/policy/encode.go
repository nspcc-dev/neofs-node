package policy

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

func Encode(p *netmap.PlacementPolicy) []string {
	if p == nil {
		return nil
	}

	var (
		replicas  = p.GetReplicas()
		selectors = p.GetSelectors()
		filters   = p.GetFilters()
	)

	// 1 for container backup factor
	result := make([]string, 0, len(replicas)+len(selectors)+len(filters)+1)

	// first print replicas
	encodeReplicas(replicas, &result)

	// then backup factor
	if backupFactor := p.GetContainerBackupFactor(); backupFactor != 0 {
		result = append(result, fmt.Sprintf("CBF %d", backupFactor))
	}

	// then selectors
	encodeSelectors(selectors, &result)

	// then filters
	encodeFilters(filters, &result)

	return result
}

func encodeReplicas(replicas []*netmap.Replica, dst *[]string) {
	builder := new(strings.Builder)

	for _, replica := range replicas {
		builder.WriteString("REP ")
		builder.WriteString(strconv.FormatUint(uint64(replica.GetCount()), 10))

		if s := replica.GetSelector(); s != "" {
			builder.WriteString(" IN ")
			builder.WriteString(s)
		}

		*dst = append(*dst, builder.String())
		builder.Reset()
	}
}

func encodeSelectors(selectors []*netmap.Selector, dst *[]string) {
	builder := new(strings.Builder)

	for _, selector := range selectors {
		builder.WriteString("SELECT ")
		builder.WriteString(strconv.FormatUint(uint64(selector.GetCount()), 10))

		if a := selector.GetAttribute(); a != "" {
			builder.WriteString(" IN")

			switch selector.GetClause() {
			case netmap.Same:
				builder.WriteString(" SAME ")
			case netmap.Distinct:
				builder.WriteString(" DISTINCT ")
			default:
				builder.WriteString(" ")
			}

			builder.WriteString(a)
		}

		if f := selector.GetFilter(); f != "" {
			builder.WriteString(" FROM ")
			builder.WriteString(f)
		}

		if n := selector.GetName(); n != "" {
			builder.WriteString(" AS ")
			builder.WriteString(n)
		}

		*dst = append(*dst, builder.String())
		builder.Reset()
	}
}

func encodeFilters(filters []*netmap.Filter, dst *[]string) {
	builder := new(strings.Builder)

	for _, filter := range filters {
		builder.WriteString("FILTER ")

		builder.WriteString(encodeFilter(filter))

		*dst = append(*dst, builder.String())
		builder.Reset()
	}
}

func operationString(operation netmap.Operation) string {
	switch operation {
	case netmap.EQ:
		return "EQ"
	case netmap.NE:
		return "NE"
	case netmap.GT:
		return "GT"
	case netmap.GE:
		return "GE"
	case netmap.LT:
		return "LT"
	case netmap.LE:
		return "LE"
	case netmap.OR:
		return "OR"
	case netmap.AND:
		return "AND"
	default:
		return ""
	}
}

func encodeFilter(filter *netmap.Filter) string {
	builder := new(strings.Builder)
	unspecified := filter.GetOp() == netmap.UnspecifiedOperation

	if k := filter.GetKey(); k != "" {
		builder.WriteString(k)
		builder.WriteString(" ")
		builder.WriteString(operationString(filter.GetOp()))
		builder.WriteString(" ")
		builder.WriteString(filter.GetValue())
	} else if n := filter.GetName(); unspecified && n != "" {
		builder.WriteString("@")
		builder.WriteString(n)
	}

	for i, subfilter := range filter.GetFilters() {
		if i != 0 {
			builder.WriteString(" ")
			builder.WriteString(operationString(filter.GetOp()))
			builder.WriteString(" ")
		}

		builder.WriteString(encodeFilter(subfilter))
	}

	if n := filter.GetName(); n != "" && !unspecified {
		builder.WriteString(" AS ")
		builder.WriteString(n)
	}

	return builder.String()
}
