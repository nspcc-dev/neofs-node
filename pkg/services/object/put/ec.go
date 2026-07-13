package putsvc

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

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

// finalizeErr must not be called concurrently with any other ecProgress's methods.
// call it only if at least one part was not put successfully.
func (p *ecProgress) finalizeErr(err *errIncompletePut) error {
	if p.stop {
		return fmt.Errorf("not enough nodes for EC parts (%d parts were put)", p.successPuts.Load())
	}
	err.singleErr = fmt.Errorf("only %d EC parts were put successfully"+
		" for %s EC rule, latest error: %w", p.successPuts.Load(), p.ecRule, err.singleErr)
	return err
}

func (t *distributedTarget) applyECRule(signer neofscrypto.Signer, obj object.Object, ruleIdx int, payloadParts [][]byte, ecRule iec.Rule, nodeList []netmap.NodeInfo) error {
	var (
		eg   errgroup.Group
		prog = newECProgress(nodeList, ecRule)
	)

	for partIdx := range payloadParts {
		eg.Go(func() error {
			if err := t.formAndSaveObjectForECPart(prog, signer, obj, ruleIdx, partIdx, payloadParts, nodeList); err != nil {
				return fmt.Errorf("form and save object for part %d: %w", partIdx, err)
			}

			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		var incompleteErr errIncompletePut
		if errors.As(err, &incompleteErr) {
			return prog.finalizeErr(&incompleteErr)
		}

		return err
	}

	return nil
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

	var metaC *metaCollection
	if t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
		tx, dataToSign, err := t.encodeObjectMetadata(partObj)
		if err != nil {
			return fmt.Errorf("encode object metadata: %w", err)
		}
		metaC = &metaCollection{
			metaTransaction: tx,
			dataToSign:      dataToSign,
			signatures:      make([][]meta.IndexedSignature, len(t.containerNodes.PrimaryCounts())+len(t.containerNodes.ECRules())),
		}
	}

	if err := t.saveECPartWithProgress(prog, partObj, encObj, ruleIdx, partIdx, len(payloadParts), nodeList, metaC); err != nil {
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

func (t *distributedTarget) saveECPart(part object.Object, encObj encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo,
	metaC *metaCollection) error {
	return t.distributeObjectWithMeta(part, encObj, metaC, func(obj object.Object, encObj encodedObject) error {
		return t.distributeECPart(nil, obj, encObj, ruleIdx, partIdx, totalParts, nodeList, metaC)
	})
}

func (t *distributedTarget) saveECPartWithProgress(prog *ecProgress, part object.Object, encObj encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo, metaC *metaCollection) error {
	return t.distributeObjectWithMeta(part, encObj, metaC, func(obj object.Object, encObj encodedObject) error {
		return t.distributeECPart(prog, obj, encObj, ruleIdx, partIdx, totalParts, nodeList, metaC)
	})
}

func (t *distributedTarget) distributeECPart(prog *ecProgress, part object.Object, enc encodedObject, ruleIdx, partIdx, totalParts int, nodeList []netmap.NodeInfo, metaC *metaCollection) error {
	var firstErr error
	for i := range iec.NodeSequenceForPart(partIdx, totalParts, len(nodeList)) {
		if prog != nil && !prog.canTryNode(i) {
			continue
		}

		err := t.saveECPartOnNode(ruleIdx, part, enc, nodeList[i], metaC)
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
			return errIncompletePut{singleErr: firstErr}
		}
	}

	return errIncompletePut{singleErr: firstErr}
}

func (t *distributedTarget) saveECPartOnNode(ruleIdx int, obj object.Object, enc encodedObject, node netmap.NodeInfo, metaC *metaCollection) error {
	var n = nodeDesc{
		info:            node,
		local:           t.placementIterator.neoFSNet.IsLocalNodePublicKey(node.PublicKey()),
		placementVector: len(t.containerNodes.PrimaryCounts()) + ruleIdx,
	}

	return t.sendObject(obj, enc, n, metaC)
}

func ecNodesForPart(nodeList []netmap.NodeInfo, partIdx, totalParts int) []netmap.NodeInfo {
	res := make([]netmap.NodeInfo, 0, len(nodeList))
	for i := range iec.NodeSequenceForPart(partIdx, totalParts, len(nodeList)) {
		res = append(res, nodeList[i])
	}
	return res
}
