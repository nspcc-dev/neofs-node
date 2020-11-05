package policy

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
)

func Encode(p *netmap.PlacementPolicy) []string {
	if p == nil {
		return nil
	}

	var (
		replicas  = p.Replicas()
		selectors = p.Selectors()
		filters   = p.Filters()
	)

	// 1 for container backup factor
	result := make([]string, 0, len(replicas)+len(selectors)+len(filters)+1)

	// first print replicas
	encodeReplicas(replicas, &result)

	// then backup factor
	if backupFactor := p.ContainerBackupFactor(); backupFactor != 0 {
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
		builder.WriteString(strconv.FormatUint(uint64(replica.Count()), 10))

		if s := replica.Selector(); s != "" {
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
		builder.WriteString(strconv.FormatUint(uint64(selector.Count()), 10))

		if a := selector.Attribute(); a != "" {
			builder.WriteString(" IN")

			switch selector.Clause() {
			case netmap.ClauseSame:
				builder.WriteString(" SAME ")
			case netmap.ClauseDistinct:
				builder.WriteString(" DISTINCT ")
			default:
				builder.WriteString(" ")
			}

			builder.WriteString(a)
		}

		if f := selector.Filter(); f != "" {
			builder.WriteString(" FROM ")
			builder.WriteString(f)
		}

		if n := selector.Name(); n != "" {
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

func encodeFilter(filter *netmap.Filter) string {
	builder := new(strings.Builder)
	unspecified := filter.Operation() == 0

	if k := filter.Key(); k != "" {
		builder.WriteString(k)
		builder.WriteString(" ")
		builder.WriteString(filter.Operation().String())
		builder.WriteString(" ")
		builder.WriteString(filter.Value())
	} else if n := filter.Name(); unspecified && n != "" {
		builder.WriteString("@")
		builder.WriteString(n)
	}

	for i, subfilter := range filter.InnerFilters() {
		if i != 0 {
			builder.WriteString(" ")
			builder.WriteString(filter.Operation().String())
			builder.WriteString(" ")
		}

		builder.WriteString(encodeFilter(subfilter))
	}

	if n := filter.Name(); n != "" && !unspecified {
		builder.WriteString(" AS ")
		builder.WriteString(n)
	}

	return builder.String()
}
