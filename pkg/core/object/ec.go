package objectcore

import (
	"crypto/sha256"
	"encoding/hex"
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
	if !isParent && len(attrs) > 0 {
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
		if ecAttr != "" {
			return false, fmt.Errorf("object with EC attributes %s in container without EC rules", ecAttr)
		}
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
			return false, nil
		}
		if blank {
			if ecAttr != "" {
				return false, errors.New("blank object with EC attributes")
			}
			break
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
	if part.Signature() != nil {
		return errors.New("signed EC part")
	}

	if part.SessionToken() != nil {
		return errors.New("session token detected")
	}

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

	return checkECParent(*parent, part, rules, pi)
}

func checkECParent(parent, part object.Object, rules []netmap.ECRule, pi iec.PartInfo) error {
	var hashAttr string
	for _, attr := range parent.Attributes() {
		if strings.HasPrefix(attr.Key(), iec.AttributePrefix) {
			if attr.Key() != iec.AttributePartsHashes {
				return fmt.Errorf("parent object has prohibited EC %s attribute", attr.Key())
			}
			if hashAttr != "" {
				return fmt.Errorf("multiple %s EC attributes in parent object", iec.AttributePartsHashes)
			}
			hashAttr = attr.Value()
		}
	}
	if hashAttr == "" {
		return fmt.Errorf("missing %s EC attribute in parent object", iec.AttributePartsHashes)
	}

	cs, ok := part.PayloadChecksum()
	if !ok {
		return fmt.Errorf("EC part does not have payload checksum")
	}

	const sumLen = sha256.Size*2 + 1 // "<sum>,"
	var off int
	for i, rule := range rules {
		if i == pi.RuleIndex {
			off += sumLen * pi.Index
			break
		}
		off += sumLen * int(rule.DataPartNum()+rule.ParityPartNum())
	}

	if len(hashAttr) < off+sumLen-1 {
		return fmt.Errorf("parent header does not have enough checksums for EC part: EC rule index: %d, part index: %d", pi.RuleIndex, pi.Index)
	}

	csFromParentStr := hashAttr[off : off+sumLen-1] // without comma
	csStr := hex.EncodeToString(cs.Value())
	if csStr != csFromParentStr {
		return fmt.Errorf("EC part payload checksum (%s) does not equal parent's one (%s)", csStr, csFromParentStr)
	}

	return nil
}
