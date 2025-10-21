package ec

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/tzhash/tz"
)

// PartInfo groups information about single EC part produced according to some [Rule].
type PartInfo struct {
	// Index of EC rule in the container storage policy.
	RuleIndex int
	// Part index.
	Index int
}

// GetPartInfo fetches EC part info from given object header. It one of
// [AttributeRuleIdx] or [AttributeRuleIdx] attributes is set, the other must be
// set too. If both are missing, GetPartInfo returns [PartInfo.RuleIndex] = -1
// without error.
func GetPartInfo(obj object.Object) (PartInfo, error) {
	return getPartInfo(obj, false)
}

// GetRequiredPartInfo is like [GetPartInfo] but fails when object does not
// contain EC part attributes.
func GetRequiredPartInfo(obj object.Object) (PartInfo, error) {
	return getPartInfo(obj, true)
}

func getPartInfo(obj object.Object, require bool) (PartInfo, error) {
	ruleIdx, err := iobject.GetIndexAttribute(obj, AttributeRuleIdx)
	if err != nil {
		return PartInfo{}, fmt.Errorf("invalid index attribute %s: %w", AttributeRuleIdx, err)
	}

	partIdx, err := iobject.GetIndexAttribute(obj, AttributePartIdx)
	if err != nil {
		return PartInfo{}, fmt.Errorf("invalid index attribute %s: %w", AttributePartIdx, err)
	}

	if ruleIdx < 0 {
		if partIdx >= 0 {
			return PartInfo{}, fmt.Errorf("%s attribute is set while %s is not", AttributePartIdx, AttributeRuleIdx)
		}
		if require {
			return PartInfo{}, fmt.Errorf("missing %s attribute", AttributeRuleIdx)
		}
	} else if partIdx < 0 {
		return PartInfo{}, fmt.Errorf("%s attribute is set while %s is not", AttributeRuleIdx, AttributePartIdx)
	}

	return PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	}, nil
}

// FormObjectForECPart forms object for EC part produced from given parent object.
func FormObjectForECPart(signer neofscrypto.Signer, parent object.Object, part []byte, partInfo PartInfo) (object.Object, error) {
	var obj object.Object
	obj.SetVersion(parent.Version())
	obj.SetContainerID(parent.GetContainerID())
	obj.SetOwner(parent.Owner())
	obj.SetCreationEpoch(parent.CreationEpoch())
	obj.SetType(object.TypeRegular)
	obj.SetSessionToken(parent.SessionToken())

	obj.SetParent(&parent)
	iobject.SetIntAttribute(&obj, AttributeRuleIdx, partInfo.RuleIndex)
	iobject.SetIntAttribute(&obj, AttributePartIdx, partInfo.Index)

	obj.SetPayload(part)
	obj.SetPayloadSize(uint64(len(part)))
	if _, ok := parent.PayloadHomomorphicHash(); ok {
		obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(part)))
	}

	if err := obj.SetVerificationFields(signer); err != nil {
		return object.Object{}, fmt.Errorf("set verification fields: %w", err)
	}

	return obj, nil
}

// DecodePartInfoFromAttributes decodes EC part info from given object
// attributes. It one of attributes is set, the other must be set too. If both
// are missing, DecodePartInfoFromAttributes returns [PartInfo.RuleIndex] = -1
// without error.
func DecodePartInfoFromAttributes(ruleIdxAttr, partIdxAttr string) (PartInfo, error) {
	// TODO: sync with object GET server
	// TODO: sync with GetPartInfo, it does not check for numeric limits.
	if ruleIdxAttr == "" {
		if partIdxAttr != "" {
			return PartInfo{}, errors.New("part index is set, rule index is not")
		}
		return PartInfo{RuleIndex: -1}, nil
	}
	if partIdxAttr == "" {
		return PartInfo{}, errors.New("rule index is set, part index is not")
	}

	ruleIdx, err := decodeUint8StringToInt(ruleIdxAttr)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return PartInfo{}, errors.New("rule index out of range")
		}
		return PartInfo{}, fmt.Errorf("decode rule index: %w", err)
	}
	partIdx, err := decodeUint8StringToInt(partIdxAttr)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return PartInfo{}, errors.New("part index out of range")
		}
		return PartInfo{}, fmt.Errorf("decode part index: %w", err)
	}

	return PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	}, nil
}

// returns [strconv.ErrRange] if value >= 256.
func decodeUint8StringToInt(s string) (int, error) {
	n, err := strconv.ParseUint(s, 10, 8)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ObjectWithAttributes checks whether obj contains at least one EC attribute.
func ObjectWithAttributes(obj object.Object) bool {
	return slices.ContainsFunc(obj.Attributes(), func(a object.Attribute) bool {
		return strings.HasPrefix(a.Key(), AttributePrefix)
	})
}
