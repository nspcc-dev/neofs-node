package putsvc

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/klauspost/reedsolomon"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

// TODO: Place in SDK.
const (
	attrECRuleIdx = "__NEOFS__EC_RULE_IDX"
	attrECPartIdx = "__NEOFS__EC_PART_IDX"
)

// ECRule represents erasure coding rule for object encoding and placement.
type ECRule struct {
	DataParts   uint8
	ParityParts uint8
}

// String implements [fmt.Stringer].
func (x ECRule) String() string {
	return strconv.FormatUint(uint64(x.DataParts), 10) + "/" + strconv.FormatUint(uint64(x.ParityParts), 10)
}

func (t *distributedTarget) ecAndSaveObject(signer neofscrypto.Signer, obj object.Object, ecRules []ECRule, nodeLists [][]netmap.NodeInfo) error {
	for i := range ecRules {
		if slices.Contains(ecRules[:i], ecRules[i]) { // has already been processed, see below
			continue
		}

		payloadParts, err := ecPayload(ecRules[i], obj.Payload())
		if err != nil {
			return fmt.Errorf("split object payload into EC parts for rule #%d (%s): %w", i, ecRules[i], err)
		}

		if err := t.formAndSaveECParts(signer, obj, i, payloadParts, nodeLists[i]); err != nil {
			return fmt.Errorf("encode and save EC parts for rule #%d (%s): %w", i, ecRules[i], err)
		}

		for j := i + 1; j < len(ecRules); j++ {
			if ecRules[i] != ecRules[j] {
				continue
			}
			if err := t.formAndSaveECParts(signer, obj, j, payloadParts, nodeLists[j]); err != nil {
				return fmt.Errorf("encode and save EC parts for rule #%d (%s): %w", j, ecRules[j], err)
			}
		}
	}

	return nil
}

func (t *distributedTarget) formAndSaveECParts(signer neofscrypto.Signer, obj object.Object, ruleIdx int, payloadParts [][]byte, nodeList []netmap.NodeInfo) error {
	parts, err := formECParts(signer, obj, ruleIdx, payloadParts)
	if err != nil {
		return fmt.Errorf("form: %w", err)
	}

	for i := range parts {
		// TODO: calc encodedObject
		// TODO: use proper metadata
		// TODO: put parts in parallel
		err := t.saveECPart(parts[i], objectcore.ContentMeta{}, encodedObject{}, i, len(parts), nodeList)
		if err != nil {
			return fmt.Errorf("save part #%d: %w", i, err)
		}
	}

	return nil
}

func (t *distributedTarget) saveECPart(part object.Object, objMeta objectcore.ContentMeta, encObj encodedObject, idx, total int, nodeList []netmap.NodeInfo) error {
	return t.distributeObject(part, objMeta, encObj, func(obj object.Object, objMeta objectcore.ContentMeta, encObj encodedObject) error {
		return t.distributeECPart(obj, objMeta, encObj, idx, total, nodeList)
	})
}

func (t *distributedTarget) distributeECPart(part object.Object, objMeta objectcore.ContentMeta, enc encodedObject, idx, total int, nodeList []netmap.NodeInfo) error {
	var firstErr error
	for {
		err := t.saveECPartOnNode(part, objMeta, enc, nodeList[idx])
		if err == nil {
			return nil
		}

		na := slices.Collect(nodeList[idx].NetworkEndpoints())
		if firstErr == nil {
			firstErr = fmt.Errorf("save on SN #%d (%s): %w", idx, na, err)
		} else {
			t.placementIterator.log.Info("failed to save EC part on reserve SN", zap.Error(err), zap.Strings("addresses", na))
		}

		if idx += total; idx >= len(nodeList) {
			return errIncompletePut{singleErr: firstErr}
		}
	}
}

func (t *distributedTarget) saveECPartOnNode(obj object.Object, objMeta objectcore.ContentMeta, enc encodedObject, node netmap.NodeInfo) error {
	var n nodeDesc
	n.local = t.placementIterator.neoFSNet.IsLocalNodePublicKey(node.PublicKey())
	if !n.local {
		var err error
		if n.info, err = convertNodeInfo(node); err != nil {
			return fmt.Errorf("convert node info: %w", err)
		}
	}

	return t.sendObject(obj, objMeta, enc, n)
}

func formECParts(signer neofscrypto.Signer, obj object.Object, ruleIdx int, payloadParts [][]byte) ([]object.Object, error) {
	var blankPart object.Object
	blankPart.SetVersion(obj.Version())
	blankPart.SetContainerID(obj.GetContainerID())
	blankPart.SetOwner(obj.Owner())
	blankPart.SetCreationEpoch(obj.CreationEpoch())
	blankPart.SetType(object.TypeRegular)
	blankPart.SetSessionToken(obj.SessionToken())
	blankPart.SetParent(&obj)

	_, withTZHash := obj.PayloadHomomorphicHash()

	var err error
	res := make([]object.Object, len(payloadParts))
	for i := range payloadParts {
		res[i] = blankPart
		if err = finalizeECPart(&res[i], signer, payloadParts[i], ruleIdx, i, withTZHash); err != nil {
			return nil, fmt.Errorf("form object for payload part #%d: %w", i, err)
		}
	}

	return res, nil
}

func finalizeECPart(blank *object.Object, signer neofscrypto.Signer, payloadPart []byte, ruleIdx, partIdx int, withTZHash bool) error {
	blank.SetPayload(payloadPart)
	blank.SetPayloadSize(uint64(len(payloadPart)))
	if withTZHash {
		blank.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(payloadPart)))
	}
	blank.SetAttributes(
		object.NewAttribute(attrECPartIdx, strconv.Itoa(partIdx)),
		object.NewAttribute(attrECRuleIdx, strconv.Itoa(ruleIdx)),
	)

	if err := blank.SetVerificationFields(signer); err != nil {
		return err
	}

	return nil
}

func ecPayload(rule ECRule, payload []byte) ([][]byte, error) {
	if len(payload) == 0 {
		return make([][]byte, rule.DataParts+rule.ParityParts), nil
	}

	// TODO: Explore reedsolomon.Option for performance improvement.
	enc, err := reedsolomon.New(int(rule.DataParts), int(rule.ParityParts))
	if err != nil { // should never happen
		return nil, fmt.Errorf("init Reed-Solomon encoder: %w", err)
	}

	parts, err := enc.Split(payload)
	if err != nil {
		return nil, fmt.Errorf("split data: %w", err)
	}

	if err := enc.Encode(parts); err != nil {
		return nil, fmt.Errorf("calculate Reed-Solomon parity: %w", err)
	}

	return parts, nil
}

type ecPartInfo struct {
	ruleIdx int
	partIdx int
}

func getECPartInfo(hdr object.Object) (ecPartInfo, error) {
	ruleIdx, err := getIntAttribute(hdr, attrECRuleIdx)
	if err != nil {
		return ecPartInfo{}, fmt.Errorf("invalid int attribute %s: %w", attrECRuleIdx, err)
	}

	partIdx, err := getIntAttribute(hdr, attrECPartIdx)
	if err != nil {
		return ecPartInfo{}, fmt.Errorf("invalid int attribute %s: %w", attrECPartIdx, err)
	}

	if ruleIdx < 0 {
		if partIdx >= 0 {
			return ecPartInfo{}, fmt.Errorf("%s attribute is set while %s is not", attrECPartIdx, attrECRuleIdx)
		}
	} else if partIdx < 0 {
		return ecPartInfo{}, fmt.Errorf("%s attribute is set while %s is not", attrECRuleIdx, attrECPartIdx)
	}

	return ecPartInfo{
		ruleIdx: ruleIdx,
		partIdx: partIdx,
	}, nil
}

func getIntAttribute(obj object.Object, attr string) (int, error) {
	attrs := obj.Attributes()
	for i := range attrs {
		if attrs[i].Key() == attr {
			return strconv.Atoi(attrs[i].Value())
		}
	}

	return -1, nil
}
