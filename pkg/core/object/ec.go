package object

import (
	"errors"
	"fmt"
	"strings"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

func checkEC(hdr object.Object, rules []netmap.ECRule, blank bool, isParent bool) (bool, error) {
	attrs := hdr.Attributes()
	var ecAttr string
	if len(attrs) > 0 {
		if first := attrs[0].Key(); strings.HasPrefix(first, iec.AttributePrefix) {
			for i := 1; i < len(attrs); i++ {
				if !strings.HasPrefix(attrs[i].Key(), iec.AttributePrefix) {
					return false, fmt.Errorf("mix of EC (%s) and non-EC (%s) attributes", first, attrs[i].Key())
				}
			}

			ecAttr = first
		} else {
			for i := 1; i < len(attrs); i++ {
				if strings.HasPrefix(attrs[i].Key(), iec.AttributePrefix) {
					return false, fmt.Errorf("mix of EC (%s) and non-EC (%s) attributes", attrs[i].Key(), first)
				}
			}
		}
	}

	if len(rules) == 0 {
		return false, nil
	}

	switch typ := hdr.Type(); typ {
	default:
		return false, fmt.Errorf("unsupported object type %v", typ)
	case object.TypeTombstone, object.TypeLock, object.TypeLink:
		if ecAttr != "" {
			return false, fmt.Errorf("%s object with EC attribute %s", typ, ecAttr)
		}
	case object.TypeRegular:
		if isParent {
			if ecAttr != "" {
				return false, fmt.Errorf("parent object has EC attribute %s", ecAttr)
			}
			return false, nil
		}

		if blank {
			return false, errors.New("blank object with EC attributes")
		}

		if ecAttr == "" {
			return false, errors.New("missing EC attributes in regular object")
		}

		if err := checkECPart(hdr, rules); err != nil {
			return false, fmt.Errorf("invalid regular EC part object: %w", err)
		}
	}

	return ecAttr != "", nil
}

func checkECPart(part object.Object, rules []netmap.ECRule) error {
	parent := part.Parent()
	if parent == nil {
		return errors.New("missing parent header")
	}

	if parentVal, partVal := parent.Version(), part.Version(); !equalProtoVersions(parentVal, partVal) {
		return fmt.Errorf("diff proto version in parent (%s) and part (%s)", stringifyVersion(parentVal), stringifyVersion(partVal))
	}

	if parentVal, partVal := parent.GetContainerID(), part.GetContainerID(); parentVal != partVal {
		return fmt.Errorf("diff container in parent (%s) and part (%s)", parentVal, partVal)
	}

	if parentVal, partVal := parent.Owner(), part.Owner(); parentVal != partVal {
		return fmt.Errorf("diff owner in parent (%s) and part (%s)", parentVal, partVal)
	}

	if parentVal, partVal := parent.CreationEpoch(), part.CreationEpoch(); parentVal != partVal {
		return fmt.Errorf("diff creation epoch in parent (%d) and part (%d)", parentVal, partVal)
	}

	_, parentHH := parent.PayloadHomomorphicHash()
	_, partHH := part.PayloadHomomorphicHash()
	if parentHH != partHH {
		return fmt.Errorf("diff homomorphic hash presence in parent (%t) and part (%t)", parentHH, partHH)
	}

	pi, err := iec.GetRequiredPartInfo(part)
	if err != nil {
		return fmt.Errorf("unavailable part info: %w", err)
	}
	if pi.RuleIndex >= len(rules) {
		return fmt.Errorf("rule index attribute (%d) overflows total number of rules in policy (%d)", pi.RuleIndex, len(rules))
	}

	rule := rules[pi.RuleIndex]
	dataPartNum := rule.DataPartNum()
	if total := dataPartNum + rule.ParityPartNum(); pi.Index >= int(total) {
		return fmt.Errorf("part index attribute (%d) overflows total number of parts in policy (%d)", pi.Index, total)
	}

	parentPldLen := parent.PayloadSize()
	partPldLen := part.PayloadSize()

	if exp := (parentPldLen + uint64(dataPartNum) - 1) / uint64(dataPartNum); exp != partPldLen {
		return fmt.Errorf("wrong part payload len: expected %d, got %d, parent %d", exp, partPldLen, parentPldLen)
	}

	return nil
}
