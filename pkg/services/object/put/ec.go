package putsvc

import (
	"encoding/base64"
	"encoding/hex"
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
	ecAttrRuleIdx = "__NEOFS__EC_RULE_IDX"

	reedSolomonAttrIdx              = "__NEOFS__EC_RS_IDX"
	reedSolomonAttrSrcID            = "__NEOFS__EC_RS_SRC_ID"
	reedSolomonAttrSrcPayloadLen    = "__NEOFS__EC_RS_SRC_PAYLOAD_LEN"
	reedSolomonAttrSrcPayloadSHA256 = "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_SHA256"
	reedSolomonAttrSrcPayloadTZ     = "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_TZ"
	reedSolomonAttrSrcSignature     = "__NEOFS__EC_RS_SRC_SIGNATURE"
)

// TODO: docs.
type ECRule struct {
	DataShards   uint8
	ParityShards uint8
}

// TODO: docs.
func (x ECRule) String() string {
	return strconv.FormatUint(uint64(x.DataShards), 10) + "/" + strconv.FormatUint(uint64(x.ParityShards), 10)
}

func (t *distributedTarget) ecAndSaveObject(obj object.Object, ecRules []ECRule, nodeLists [][]netmap.NodeInfo) error {
	// FIXME: object may already be an EC part, then it must not be split
	var mn map[string]error
	for i := range ecRules {
		if slices.Contains(ecRules[:i], ecRules[i]) { // has already been processed, see below
			continue
		}

		parts, err := ecObject(t.sessionSigner, obj, i, ecRules[i])
		if err != nil {
			return fmt.Errorf("split object into EC parts by rule #%d (%s): %w", i, ecRules[i], err)
		}

		nextSameRule := slices.Index(ecRules[i+1:], ecRules[i])
		if nextSameRule >= 0 {
			if mn == nil {
				mn = make(map[string]error, len(nodeLists))
			} else {
				clear(mn)
			}
		} else {
			mn = nil
		}

		if err := t.saveECParts(parts, nodeLists[i], mn); err != nil {
			return fmt.Errorf("save EC parts by rule #%d (%s): %w", i, ecRules[i], err)
		}

		if nextSameRule < 0 {
			continue
		}

		for j := nextSameRule; j < len(ecRules); j++ {
			if ecRules[i] != ecRules[j] {
				continue
			}
			// partitioning is the same, location is different in general
			// TODO: need EC rule num in attributes?
			if err := t.saveECParts(parts, nodeLists[j], mn); err != nil {
				return fmt.Errorf("save EC parts by rule #%d (%s): %w", j, ecRules[j], err)
			}
		}
	}

	return nil
}

func (t *distributedTarget) saveECParts(parts []object.Object, nodeList []netmap.NodeInfo, mn map[string]error) error {
	for i := range parts {
		// FIXME: calc encodedObject
		// FIXME: use proper metadata
		// TODO: put parts in parallel
		err := t.saveECPart(parts[i], objectcore.ContentMeta{}, encodedObject{}, i, len(parts), nodeList, mn)
		if err != nil {
			return fmt.Errorf("save part #%d: %w", i, err)
		}
	}

	return nil
}

func (t *distributedTarget) saveECPart(part object.Object, objMeta objectcore.ContentMeta, encObj encodedObject, idx, total int, nodeList []netmap.NodeInfo, mn map[string]error) error {
	return t.distributeObject(part, objMeta, encObj, func(obj object.Object, objMeta objectcore.ContentMeta, encObj encodedObject) error {
		return t.distributeECPart(obj, objMeta, encObj, idx, total, nodeList, mn)
	})
}

func (t *distributedTarget) distributeECPart(part object.Object, objMeta objectcore.ContentMeta, enc encodedObject, idx, total int, nodeList []netmap.NodeInfo, mn map[string]error) error {
	var firstErr error
	var err error
	for {
		if mn != nil {
			k := string(nodeList[idx].PublicKey())
			var ok bool
			if err, ok = mn[k]; !ok {
				err = t.saveECPartOnNode(part, objMeta, enc, nodeList[idx])
				mn[k] = err
			}
		} else {
			err = t.saveECPartOnNode(part, objMeta, enc, nodeList[idx])
		}
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

func ecObject(signer neofscrypto.Signer, obj object.Object, ruleIdx int, rule ECRule) ([]object.Object, error) {
	// TODO: Explore possibility to reset and reuse encoder for next object.
	// TODO: Explore reedsolomon.Option for performance improvement.
	// TODO: Explore reedsolomon.StreamEncoder.
	enc, err := reedsolomon.New(int(rule.DataShards), int(rule.ParityShards))
	if err != nil { // should never happen
		return nil, fmt.Errorf("init Reed-Solomon encoder: %w", err)
	}
	payloadParts, err := enc.Split(obj.Payload())
	if err != nil {
		return nil, fmt.Errorf("split data: %w", err)
	}
	if err := enc.Encode(payloadParts); err != nil {
		return nil, fmt.Errorf("calculate Reed-Solomon parity: %w", err)
	}

	srcIDAttr := obj.GetID().String()
	srcPldLenAttr := strconv.FormatUint(obj.PayloadSize(), 10)
	var srcPldSHA256Attr string
	if cs, ok := obj.PayloadChecksum(); ok {
		srcPldSHA256Attr = hex.EncodeToString(cs.Value())
	}
	var srcPldTZAttr string
	if cs, ok := obj.PayloadHomomorphicHash(); ok {
		srcPldTZAttr = hex.EncodeToString(cs.Value())
	}
	var srcSigAttr string
	if sig := obj.Signature(); sig != nil {
		srcSigAttr = base64.StdEncoding.EncodeToString(sig.Marshal())
	}

	res := make([]object.Object, len(payloadParts))
	for i := range payloadParts {
		res[i], err = formECPart(obj, ruleIdx, i, payloadParts[i], signer, srcIDAttr, srcPldLenAttr, srcPldSHA256Attr, srcPldTZAttr, srcSigAttr)
		if err != nil {
			return nil, fmt.Errorf("form part #%d: %w", i, err)
		}
	}

	return res, nil
}

func formECPart(hdr object.Object, ruleIdx, partIdx int, payloadPart []byte, signer neofscrypto.Signer, srcIDAttr, srcPldLenAttr, srcPldSHA256Attr, srcPldTZAttr, srcSigAttr string) (object.Object, error) {
	attrs := hdr.Attributes() // TODO: clone may be needed, check is it safe

	attrs = append(attrs,
		object.NewAttribute(ecAttrRuleIdx, strconv.Itoa(ruleIdx)),
		object.NewAttribute(reedSolomonAttrIdx, strconv.Itoa(partIdx)),
		object.NewAttribute(reedSolomonAttrSrcID, srcIDAttr),
		object.NewAttribute(reedSolomonAttrSrcPayloadLen, srcPldLenAttr),
	)
	if srcPldSHA256Attr != "" {
		attrs = append(attrs, object.NewAttribute(reedSolomonAttrSrcPayloadSHA256, srcPldSHA256Attr))
	}
	if srcPldTZAttr != "" {
		attrs = append(attrs, object.NewAttribute(reedSolomonAttrSrcPayloadTZ, srcPldTZAttr))
	}
	if srcSigAttr != "" {
		attrs = append(attrs, object.NewAttribute(reedSolomonAttrSrcSignature, srcSigAttr))
	}

	hdr.SetAttributes(attrs...)

	hdr.SetPayloadSize(uint64(len(payloadPart)))
	hdr.SetPayloadChecksum(object.CalculatePayloadChecksum(payloadPart))
	if srcPldTZAttr != "" {
		hdr.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(payloadPart)))
	}
	hdr.SetPayload(payloadPart)

	if err := hdr.SetIDWithSignature(signer); err != nil {
		return object.Object{}, fmt.Errorf("sign: %w", err)
	}

	return hdr, nil
}

type ecPartInfo struct {
	ruleIdx int
	partIdx int
}

func getECPartInfo(hdr object.Object) (ecPartInfo, error) {
	ruleIdx, err := getIntAttribute(hdr, ecAttrRuleIdx)
	if err != nil {
		return ecPartInfo{}, fmt.Errorf("invalid int attribute %s: %w", ecAttrRuleIdx, err)
	}

	partIdx, err := getIntAttribute(hdr, reedSolomonAttrIdx)
	if err != nil {
		return ecPartInfo{}, fmt.Errorf("invalid int attribute %s: %w", reedSolomonAttrIdx, err)
	}

	if ruleIdx < 0 {
		if partIdx >= 0 {
			return ecPartInfo{}, fmt.Errorf("%s attribute is set while %s is not", reedSolomonAttrIdx, ecAttrRuleIdx)
		}
	} else if partIdx < 0 {
		return ecPartInfo{}, fmt.Errorf("%s attribute is set while %s is not", ecAttrRuleIdx, reedSolomonAttrIdx)
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
