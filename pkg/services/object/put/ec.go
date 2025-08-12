package putsvc

import (
	"fmt"
	"math"
	"slices"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (t *distributedTarget) ecAndSaveObject(signer neofscrypto.Signer, obj object.Object, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo) error {
	for i := range ecRules {
		if slices.Contains(ecRules[:i], ecRules[i]) { // has already been processed, see below
			continue
		}

		payloadParts, err := iec.Encode(ecRules[i], obj.Payload())
		if err != nil {
			return fmt.Errorf("split object payload into EC parts for rule #%d (%s): %w", i, ecRules[i], err)
		}

		if err := t.applyECRule(signer, obj, i, payloadParts, nodeLists[i]); err != nil {
			return fmt.Errorf("apply EC rule #%d (%s): %w", i, ecRules[i], err)
		}

		for j := i + 1; j < len(ecRules); j++ {
			if ecRules[i] != ecRules[j] {
				continue
			}
			if err := t.applyECRule(signer, obj, j, payloadParts, nodeLists[j]); err != nil {
				return fmt.Errorf("apply EC rule #%d (%s): %w", j, ecRules[j], err)
			}
		}
	}

	return nil
}

func (t *distributedTarget) applyECRule(signer neofscrypto.Signer, obj object.Object, ruleIdx int, payloadParts [][]byte, nodeList []netmap.NodeInfo) error {
	for partIdx := range payloadParts {
		// TODO: each part is handled independently, so this worth concurrent execution. https://github.com/nspcc-dev/neofs-node/issues/3504
		//  Note that distributeTarget.distributeObject is not thread-safe.
		if err := t.formAndSaveObjectForECPart(signer, obj, ruleIdx, partIdx, payloadParts, nodeList); err != nil {
			return fmt.Errorf("form and save object for part %d: %w", partIdx, err)
		}
	}

	return nil
}

func (t *distributedTarget) formAndSaveObjectForECPart(signer neofscrypto.Signer, obj object.Object, ruleIdx, partIdx int, payloadParts [][]byte, nodeList []netmap.NodeInfo) error {
	partObj, err := iec.FormObjectForECPart(signer, obj, payloadParts[partIdx], iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	if err != nil {
		return fmt.Errorf("form object for part: %w", err)
	}

	var encObj encodedObject
	// similar to pkg/services/object/put/distributed.go:95
	if t.localNodeInContainer {
		payloadLen := partObj.PayloadSize()
		if payloadLen > math.MaxInt {
			return fmt.Errorf("too big payload of physically stored for this server %d > %d", payloadLen, math.MaxInt)
		}

		hdr := partObj
		hdr.SetPayload(nil)

		if t.localOnly {
			encObj, err = encodeObjectWithoutPayload(hdr, int(payloadLen))
		} else {
			encObj, err = encodeReplicateRequestWithoutPayload(t.localNodeSigner, hdr, int(payloadLen), t.metainfoConsistencyAttr != "")
		}
		if err != nil {
			return fmt.Errorf("encode object into binary: %w", err)
		}

		defer putPayload(encObj.b)

		encObj.b = append(encObj.b, partObj.Payload()...)
	}

	if err := t.saveECPart(partObj, objectcore.ContentMeta{}, encObj, partIdx, len(payloadParts), nodeList); err != nil {
		return fmt.Errorf("save part object: %w", err)
	}

	return nil
}

func (t *distributedTarget) saveECPart(part object.Object, objMeta objectcore.ContentMeta, encObj encodedObject, idx, total int, nodeList []netmap.NodeInfo) error {
	return t.distributeObject(part, objMeta, encObj, func(obj object.Object, objMeta objectcore.ContentMeta, encObj encodedObject) error {
		return t.distributeECPart(obj, objMeta, encObj, idx, total, nodeList)
	})
}

func (t *distributedTarget) distributeECPart(part object.Object, objMeta objectcore.ContentMeta, enc encodedObject, partIdx, totalParts int, nodeList []netmap.NodeInfo) error {
	var firstErr error
	for i := range iec.NodeSequenceForPart(partIdx, totalParts, len(nodeList)) {
		err := t.saveECPartOnNode(part, objMeta, enc, nodeList[i])
		if err == nil {
			return nil
		}

		// err contains network addresses
		if firstErr == nil {
			firstErr = fmt.Errorf("save on SN #%d: %w", i, err)
		} else {
			t.placementIterator.log.Info("failed to save EC part on reserve SN", zap.Int("nodeIdx", i), zap.Error(err))
		}
	}

	return errIncompletePut{singleErr: firstErr}
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
