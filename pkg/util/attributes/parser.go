package attributes

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

const (
	pairSeparator     = "/"
	keyValueSeparator = ":"
)

var (
	errEmptyChain      = errors.New("empty attribute chain")
	errNonUniqueBucket = errors.New("attributes must contain unique keys")
	errUnexpectedKey   = errors.New("attributes contain unexpected key")
)

// ParseV2Attributes parses strings like "K1:V1/K2:V2/K3:V3" into netmap
// attributes.
func ParseV2Attributes(attrs []string, excl []string) ([]*netmap.Attribute, error) {
	restricted := make(map[string]struct{}, len(excl))
	for i := range excl {
		restricted[excl[i]] = struct{}{}
	}

	cache := make(map[string]*netmap.Attribute, len(attrs))

	for i := range attrs {
		line := strings.Trim(attrs[i], pairSeparator)
		chain := strings.Split(line, pairSeparator)
		if len(chain) == 0 {
			return nil, errEmptyChain
		}

		var parentKey string // backtrack parents in next pairs

		for j := range chain {
			pair := strings.Split(chain[j], keyValueSeparator)
			if len(pair) != 2 {
				return nil, fmt.Errorf("incorrect attribute pair %s", chain[j])
			}

			key := pair[0]
			value := pair[1]

			if at, ok := cache[key]; ok && at.GetValue() != value {
				return nil, errNonUniqueBucket
			}

			if _, ok := restricted[key]; ok {
				return nil, errUnexpectedKey
			}

			attribute := new(netmap.Attribute)
			attribute.SetKey(key)
			attribute.SetValue(value)

			if parentKey != "" {
				attribute.SetParents([]string{parentKey})
			}

			parentKey = key
			cache[key] = attribute
		}
	}

	result := make([]*netmap.Attribute, 0, len(cache))
	for _, v := range cache {
		result = append(result, v)
	}

	return result, nil
}
