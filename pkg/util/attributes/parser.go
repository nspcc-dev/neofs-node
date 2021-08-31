package attributes

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
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
// attributes. Supports escaped symbols "\:", "\/" and "\\".
func ParseV2Attributes(attrs []string, excl []string) ([]*netmap.NodeAttribute, error) {
	restricted := make(map[string]struct{}, len(excl))
	for i := range excl {
		restricted[excl[i]] = struct{}{}
	}

	cache := make(map[string]*netmap.NodeAttribute, len(attrs))

	for i := range attrs {
		line := strings.Trim(attrs[i], pairSeparator)
		line = replaceEscaping(line, false) // replaced escaped symbols with non-printable symbols
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

			attribute, present := cache[key]
			if present && attribute.Value() != value {
				return nil, errNonUniqueBucket
			}

			if _, ok := restricted[key]; ok {
				return nil, errUnexpectedKey
			}

			if !present {
				// replace non-printable symbols with escaped symbols without escape character
				key = replaceEscaping(key, true)
				value = replaceEscaping(value, true)

				attribute = netmap.NewNodeAttribute()
				attribute.SetKey(key)
				attribute.SetValue(value)

				cache[key] = attribute
			}

			if parentKey != "" {
				parentKeys := attribute.ParentKeys()
				if !hasString(parentKeys, parentKey) {
					attribute.SetParentKeys(append(parentKeys, parentKey)...)
				}
			}

			parentKey = key
		}
	}

	result := make([]*netmap.NodeAttribute, 0, len(cache))
	for _, v := range cache {
		result = append(result, v)
	}

	return result, nil
}

func replaceEscaping(target string, rollback bool) (s string) {
	const escChar = `\`

	var (
		oldPairSep = escChar + pairSeparator
		oldKVSep   = escChar + keyValueSeparator
		oldEsc     = escChar + escChar
		newPairSep = string(uint8(1))
		newKVSep   = string(uint8(2))
		newEsc     = string(uint8(3))
	)

	if rollback {
		oldPairSep, oldKVSep, oldEsc = newPairSep, newKVSep, newEsc
		newPairSep = pairSeparator
		newKVSep = keyValueSeparator
		newEsc = escChar
	}

	s = strings.ReplaceAll(target, oldEsc, newEsc)
	s = strings.ReplaceAll(s, oldPairSep, newPairSep)
	s = strings.ReplaceAll(s, oldKVSep, newKVSep)

	return
}

func hasString(lst []string, v string) bool {
	for i := range lst {
		if lst[i] == v {
			return true
		}
	}
	return false
}
