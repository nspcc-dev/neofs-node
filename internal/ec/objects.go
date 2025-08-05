package ec

import (
	"fmt"

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
