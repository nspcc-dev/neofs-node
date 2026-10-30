package putsvc

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// errECPartPlacement reports failure to store a single EC part on any node.
// The result of the whole EC rule is determined after all parts are processed.
var errECPartPlacement = errors.New("could not place EC part")

func newECProgress(nodeList []netmap.NodeInfo, ecRule iec.Rule) *ecProgress {
	return &ecProgress{
		nodes:      nodeList,
		takenNodes: make([]int, 0, len(nodeList)),
		ecRule:     ecRule,
	}
}

type ecProgress struct {
	ecRule iec.Rule
	nodes  []netmap.NodeInfo

	m          sync.Mutex
	takenNodes []int
	stop       bool
	failedPuts int

	successPuts atomic.Int32
}

func (p *ecProgress) canTryNode(i int) bool {
	p.m.Lock()
	defer p.m.Unlock()

	if p.stop {
		return false
	}
	if slices.Contains(p.takenNodes, i) {
		return false
	}
	p.takenNodes = append(p.takenNodes, i)
	return true
}

// returned value defines whether there are more nodes left to try.
func (p *ecProgress) submitNodeFailure() bool {
	p.m.Lock()
	if p.stop {
		p.m.Unlock()
		return false
	}
	p.failedPuts++
	placementFailed := len(p.nodes)-p.failedPuts < int(p.ecRule.DataPartNum)
	p.stop = placementFailed
	p.m.Unlock()

	return !placementFailed
}

func (p *ecProgress) submitSuccess() {
	p.successPuts.Add(1)
}

func (t *distributedTarget) applyECRule(signer neofscrypto.Signer, obj object.Object, ruleIdx int, payloadParts [][]byte, ecRule iec.Rule, minParts uint32, nodeList []netmap.NodeInfo) error {
	var (
		wg      sync.WaitGroup
		prog    = newECProgress(nodeList, ecRule)
		partErr = make([]error, len(payloadParts))
	)

	for partIdx := range payloadParts {
		wg.Go(func() {
			if err := t.formAndSaveObjectForECPart(prog, signer, obj, ruleIdx, partIdx, payloadParts, nodeList); err != nil {
				partErr[partIdx] = fmt.Errorf("form and save object for part %d: %w", partIdx, err)
			}
		})
	}
	wg.Wait()

	if t.opCtx != nil && t.opCtx.Err() != nil {
		return t.opCtx.Err()
	}

	if t.localOnly {
		// Local PUT does not perform distributed placement or count its successes.
		return errors.Join(partErr...)
	}
	return prog.result(partErr, minParts)
}

func (p *ecProgress) result(partErr []error, minParts uint32) error {
	var placementErr error
	for _, err := range partErr {
		if err == nil {
			continue
		}
		if !errors.Is(err, errECPartPlacement) {
			return err
		}
		placementErr = err
	}

	if minParts == 0 {
		minParts = uint32(len(partErr))
	}
	stored := uint32(p.successPuts.Load())
	if stored >= minParts {
		return nil
	}
	if stored < uint32(p.ecRule.DataPartNum) {
		return fmt.Errorf("not enough nodes for EC parts (%d parts were put)", stored)
	}
	return newCompletionError(errIncompletePut{singleErr: fmt.Errorf("only %d EC parts were put successfully"+
		" for %s EC rule, latest error: %w", stored, p.ecRule, placementErr)}, true)
}

func (t *distributedTarget) formAndSaveObjectForECPart(prog *ecProgress, signer neofscrypto.Signer, obj object.Object, ruleIdx, partIdx int, payloadParts [][]byte, nodeList []netmap.NodeInfo) error {
	partObj, err := formObjectForECPart(signer, obj, ruleIdx, partIdx, payloadParts)
	if err != nil {
		return err
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

	if err := t.saveECPartWithProgress(prog, partObj, encObj, ruleIdx, partIdx, len(payloadParts), nodeList); err != nil {
		return fmt.Errorf("save part object: %w", err)
	}

	return nil
}

func formObjectForECPart(signer neofscrypto.Signer, obj object.Object, ruleIdx, partIdx int, payloadParts [][]byte) (object.Object, error) {
	partObj, err := iec.FormObjectForECPart(signer, obj, payloadParts[partIdx], iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	if err != nil {
		return object.Object{}, fmt.Errorf("form object for part: %w", err)
	}

	return partObj, nil
}

func (t *distributedTarget) saveECPart(part object.Object, encObj encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo) error {
	return t.distributeObject(part, encObj, func(obj object.Object, encObj encodedObject) error {
		return t.distributeECPart(nil, obj, encObj, ruleIdx, partIdx, totalParts, nodeList)
	})
}

func (t *distributedTarget) saveECPartWithProgress(prog *ecProgress, part object.Object, encObj encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo) error {
	return t.distributeObject(part, encObj, func(obj object.Object, encObj encodedObject) error {
		return t.distributeECPart(prog, obj, encObj, ruleIdx, partIdx, totalParts, nodeList)
	})
}

func (t *distributedTarget) distributeECPart(prog *ecProgress, part object.Object, enc encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo) error {
	var replicateV2Req replicateV2InitRequest
	defer replicateV2Req.reset()

	var firstErr error
	for i := range iec.NodeSequenceForPart(partIdx, totalParts, len(nodeList)) {
		if prog != nil && !prog.canTryNode(i) {
			continue
		}

		err := t.saveECPartOnNode(ruleIdx, part, enc, &replicateV2Req, nodeList[i])
		if err == nil {
			if prog != nil {
				prog.submitSuccess()
			}
			return nil
		}

		// err contains network addresses
		if firstErr == nil {
			firstErr = fmt.Errorf("save on SN #%d: %w", i, err)
		} else {
			t.placementIterator.log.Info("failed to save EC part on reserve SN", zap.Int("nodeIdx", i), zap.Error(err))
		}

		if prog != nil && !prog.submitNodeFailure() {
			return errors.Join(errECPartPlacement, firstErr)
		}
	}

	return errors.Join(errECPartPlacement, firstErr)
}

func (t *distributedTarget) saveECPartOnNode(ruleIdx int, obj object.Object, enc encodedObject, replicateV2InitReq *replicateV2InitRequest, node netmap.NodeInfo) error {
	var n = nodeDesc{
		info:            node,
		local:           t.placementIterator.neoFSNet.IsLocalNodePublicKey(node.PublicKey()),
		placementVector: len(t.containerNodes.PrimaryCounts()) + ruleIdx,
	}

	return t.sendObject(obj, enc, n, replicateV2InitReq)
}

func (t *distributedTarget) doNotEncodeOriginalObject(hdr *object.Object) bool {
	if hdr.Type() != object.TypeRegular {
		return false
	}
	return t.ecSplitOnlyObject
}

func ecNodesForPart(nodeList []netmap.NodeInfo, partIdx, totalParts int) []netmap.NodeInfo {
	res := make([]netmap.NodeInfo, 0, len(nodeList))
	for i := range iec.NodeSequenceForPart(partIdx, totalParts, len(nodeList)) {
		res = append(res, nodeList[i])
	}
	return res
}
