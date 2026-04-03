package putsvc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	chaincontainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type metaCollection struct {
	objectData []byte

	signaturesMtx sync.RWMutex
	signatures    [][][]byte
}

type distributedTarget struct {
	opCtx context.Context

	placementIterator placementIterator

	obj                *object.Object
	networkMagicNumber uint32
	fsState            netmapcore.StateDetailed

	cnrClient               *chaincontainer.Client
	metainfoConsistencyAttr string

	metaSvc        *meta.Meta
	metaSigner     neofscrypto.Signer
	metaCollection metaCollection

	containerNodes       ContainerNodes
	localNodeInContainer bool
	localNodeSigner      neofscrypto.Signer
	sessionSigner        neofscrypto.Signer
	// - object if localOnly
	// - replicate request if localNodeInContainer
	// - payload otherwise
	encodedObject encodedObject

	relay func(nodeDesc) error

	fmt *objectcore.FormatValidator

	localStorage      ObjectStorage
	clientConstructor ClientConstructor
	transport         Transport
	commonPrm         *svcutil.CommonPrm
	keyStorage        *svcutil.KeyStorage

	localOnly bool

	// When object from request is an EC part, ecPart.RuleIndex is >= 0.
	// Otherwise, ecPart.RuleIndex is negative.
	ecPart iec.PartInfo

	initialPolicy *netmap.InitialPlacementPolicy

	postPlacementReplicator PostPlacementReplicator
}

type nodeDesc struct {
	local           bool
	placementVector int
	info            client.NodeInfo
}

// errIncompletePut is returned if processing on a container fails.
type errIncompletePut struct {
	singleErr error // error from the last responding node
}

func (x errIncompletePut) Error() string {
	const commonMsg = "incomplete object PUT by placement"

	if x.singleErr != nil {
		return fmt.Sprintf("%s: %v", commonMsg, x.singleErr)
	}

	return commonMsg
}

func newMaxReplicasError(maxReplicas, done uint, lastRule int, overloaded bool, lastRuleErr error) error {
	err := fmt.Errorf("unreachable MaxReplicas: %d of %d replicas placed in total, aborted on rule #%d (%w)",
		done, maxReplicas, lastRule, lastRuleErr)
	return newCompletionError(err, done > 0, overloaded)
}

func (t *distributedTarget) WriteHeader(hdr *object.Object) error {
	payloadLen := hdr.PayloadSize()
	if payloadLen > math.MaxInt {
		return fmt.Errorf("too big payload of physically stored for this server %d > %d", payloadLen, math.MaxInt)
	}

	if t.localNodeInContainer {
		var err error
		if t.localOnly {
			t.encodedObject, err = encodeObjectWithoutPayload(*hdr, int(payloadLen))
		} else {
			t.encodedObject, err = encodeReplicateRequestWithoutPayload(t.localNodeSigner, *hdr, int(payloadLen), t.metainfoConsistencyAttr != "")
		}
		if err != nil {
			return fmt.Errorf("encode object into binary: %w", err)
		}
	} else if payloadLen > 0 {
		b := getPayload()
		if cap(b) < int(payloadLen) {
			putPayload(b)
			b = make([]byte, 0, payloadLen)
		}
		t.encodedObject = encodedObject{b: b}
	}

	t.obj = hdr

	return nil
}

func (t *distributedTarget) Write(p []byte) (n int, err error) {
	t.encodedObject.b = append(t.encodedObject.b, p...)

	return len(p), nil
}

func (t *distributedTarget) Close() (oid.ID, error) {
	defer func() {
		putPayload(t.encodedObject.b)
		t.encodedObject.b = nil
	}()

	t.obj.SetPayload(t.encodedObject.b[t.encodedObject.pldOff:])

	typ := t.obj.Type()
	tombOrLink := typ == object.TypeLink || typ == object.TypeTombstone

	// v2 split link object and tombstone validations are expensive routines
	// and are useless if the node does not belong to the container, since
	// another node is responsible for the validation and may decline it,
	// does not matter what this node thinks about it
	if !tombOrLink || t.localNodeInContainer {
		var err error
		if err = t.fmt.ValidateContent(t.obj); err != nil {
			return oid.ID{}, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
		}
	}

	err := t.saveObject(*t.obj, t.encodedObject)
	if err != nil {
		if errors.Is(err, apistatus.ErrIncomplete) {
			return t.obj.GetID(), err
		}
		return oid.ID{}, err
	}

	return t.obj.GetID(), nil
}

func (t *distributedTarget) saveObject(obj object.Object, encObj encodedObject) error {
	if t.localOnly && t.sessionSigner == nil {
		return t.distributeObject(obj, encObj, nil)
	}

	objNodeLists, err := t.containerNodes.SortForObject(t.obj.GetID())
	if err != nil {
		return fmt.Errorf("sort container nodes by object ID: %w", err)
	}

	// TODO: handle rules in parallel. https://github.com/nspcc-dev/neofs-node/issues/3503

	repRules := t.containerNodes.PrimaryCounts()
	fullRepRules := repRules
	ecRules := t.containerNodes.ECRules()
	if typ := obj.Type(); typ == object.TypeTombstone || typ == object.TypeLock || typ == object.TypeLink || len(obj.Children()) > 0 {
		broadcast := typ != object.TypeLock || !t.localOnly

		useRepRules := repRules
		if broadcast && len(ecRules) > 0 {
			useRepRules = make([]uint, len(repRules)+len(ecRules))
			copy(useRepRules, repRules)
			for i := range ecRules {
				useRepRules[len(repRules)+i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
			}
		}

		return t.distributeObject(obj, encObj, func(obj object.Object, encObj encodedObject) error {
			return t.placementIterator.iterateNodesForObject(obj.GetID(), useRepRules, objNodeLists, broadcast, func(node nodeDesc) error {
				return t.sendObject(obj, encObj, node, &t.metaCollection)
			})
		})
	}

	initial := t.initialPolicy != nil

	if t.ecPart.RuleIndex >= 0 { // already encoded EC part
		// part info should already be verified, so we don't prevent out-of-range panic here
		if initial && len(t.initialPolicy.ReplicaLimits()) != 0 && t.initialPolicy.ReplicaLimits()[len(repRules)+t.ecPart.RuleIndex] == 0 {
			// Client can seal objects himself, but he is unlikely to enforce placement policies.
			// Using SDK slicer as an example, the object is PUT to any SN.
			// But this object should still not be saved according to initial policy.
			// So, this is no-op.
			return nil
		}

		total := int(ecRules[t.ecPart.RuleIndex].DataPartNum + ecRules[t.ecPart.RuleIndex].ParityPartNum)
		nodes := objNodeLists[len(repRules)+t.ecPart.RuleIndex]
		return t.saveECPart(obj, encObj, t.ecPart.RuleIndex, t.ecPart.Index, total, nodes, &t.metaCollection)
	}

	if t.sessionSigner == nil {
		// Here object sealed on the client side is placed. If it's an EC part, then it's already processed.
		// Otherwise, it is a regular object replica and policy is REP+EC, so no EC should/can be performed.
		if len(repRules) == 0 {
			// already checked and must not happen, and following code is not ready for this
			panic("unexpected lack of REP rules")
		}
		ecRules = nil
	}

	var maxReplicas uint
	var ecLimits []uint32
	var ruleOrder []int
	appliedECRules := make([]bool, len(ecRules))
	ruleNum := len(repRules) + len(ecRules)

	if initial {
		initialLimits := t.initialPolicy.ReplicaLimits()
		if t.sessionSigner == nil { // same as above
			if len(initialLimits) < len(repRules) {
				return fmt.Errorf("ReplicaLimits has len %d while main policy has %d REP rules", len(initialLimits), len(repRules))
			}
			initialLimits = initialLimits[:len(repRules)]
		}

		if len(initialLimits) > 0 {
			// TODO: make ContainerNodes.PrimaryCounts() to return []uint32, and just assign
			repRules = make([]uint, len(repRules)) // recreation is required to not mutate the cache
			for i := range repRules {
				repRules[i] = uint(initialLimits[i])
			}
			ecLimits = initialLimits[len(repRules):]
		}

		maxReplicas = uint(t.initialPolicy.MaxReplicas())
		if maxReplicas > 0 && t.initialPolicy.PreferLocal() {
			ruleOrder = make([]int, 0, ruleNum)
			for i := range repRules {
				if repRules[i] > 0 {
					ruleOrder = append(ruleOrder, i)
				}
			}
			for i := range ecRules {
				if ecLimits == nil || ecLimits[i] > 0 {
					ruleOrder = append(ruleOrder, len(repRules)+i)
				}
			}
			slices.SortFunc(ruleOrder, func(a, b int) int {
				if localNodeInSet(t.placementIterator.neoFSNet, objNodeLists[a]) {
					return -1
				}
				if localNodeInSet(t.placementIterator.neoFSNet, objNodeLists[b]) {
					return 1
				}
				return 0
			})
			ruleNum = len(ruleOrder)
		}
	}

	t.resetMetaCollection()

	getRuleIdx := func(i int) int {
		if ruleOrder == nil {
			return i
		}
		return ruleOrder[i]
	}

	// TODO: check possibility of using math induction instead:
	//  MIN0 = MAX - REP1 - REP2 - ... - REPn
	//  MIN1 = MIN0 + REP1 - OK0
	//  In practice there are few rules.
	sumLimitsSinceRule := func(from int) uint {
		var res uint
		for i := from; i < ruleNum; i++ {
			ruleIdx := getRuleIdx(i)
			if ecRuleIdx := ruleIdx - len(repRules); ecRuleIdx >= 0 {
				if ecLimits != nil {
					res += uint(ecLimits[ecRuleIdx])
				} else {
					res++
				}
			} else {
				res += repRules[ruleIdx]
			}
		}
		return res
	}

	leftReplicas := maxReplicas

	handleECRule := func(ruleIdx int, ecRuleIdx int, payloadParts [][]byte) (bool, error) {
		err := t.applyECRule(t.sessionSigner, obj, ecRuleIdx, payloadParts, objNodeLists[ruleIdx])
		if err != nil {
			err = fmt.Errorf("apply EC rule #%d (%s): %w", ecRuleIdx, ecRules[ecRuleIdx], err)
			if maxReplicas == 0 {
				return false, err
			}
			if leftReplicas > sumLimitsSinceRule(ruleIdx+1) {
				return false, newMaxReplicasError(maxReplicas, maxReplicas-leftReplicas, ruleIdx, false, err)
			}
			t.placementIterator.log.Info("PUT by EC rule failure", zap.Stringer("object", obj.Address()), zap.Error(err))
			return false, nil
		}
		appliedECRules[ecRuleIdx] = true
		if maxReplicas > 0 {
			leftReplicas--
			return leftReplicas == 0, nil
		}
		return false, nil
	}

	var repProg *repProgress
	var l *zap.Logger

nextRule:
	for i := range ruleNum {
		ruleIdx := getRuleIdx(i)

		if ecRuleIdx := ruleIdx - len(repRules); ecRuleIdx >= 0 {
			if ecLimits != nil && ecLimits[ecRuleIdx] == 0 { // disabled by policy
				continue
			}

			if slices.Contains(ecRules[:ecRuleIdx], ecRules[ecRuleIdx]) { // has already been processed, see below
				continue
			}

			payloadParts, err := iec.Encode(ecRules[ecRuleIdx], obj.Payload())
			if err != nil {
				return fmt.Errorf("split object payload into EC parts for rule #%d (%s): %w", ecRuleIdx, ecRules[ecRuleIdx], err)
			}

			fin, err := handleECRule(ruleIdx, ecRuleIdx, payloadParts)
			if err != nil {
				return err
			}
			if fin {
				break
			}

			for j := ecRuleIdx + 1; j < len(ecRules); j++ {
				if ecRules[ecRuleIdx] != ecRules[j] {
					continue
				}
				fin, err := handleECRule(i, j, payloadParts)
				if err != nil {
					return err
				}
				if fin {
					break nextRule
				}
			}

			continue
		}

		if repRules[ruleIdx] == 0 { // disabled by policy
			continue
		}

		if t.localNodeInContainer && t.metainfoConsistencyAttr != "" && t.metaCollection.objectData == nil {
			t.metaCollection.objectData = t.encodeObjectMetadata(obj)
		}

		if l == nil {
			l = t.placementIterator.log.With(zap.Stringer("oid", obj.GetID()))
		}

		if repProg == nil {
			repProg = newRepProgress(objNodeLists[:len(repRules)])
		}

		var minReps, maxReps uint
		if maxReplicas > 0 {
			if sum := sumLimitsSinceRule(i + 1); leftReplicas > sum {
				minReps = leftReplicas - sum
			}
			maxReps = min(repRules[ruleIdx], leftReplicas)
		} else {
			minReps, maxReps = repRules[ruleIdx], repRules[ruleIdx]
		}

		stored, overloaded, err := t.placementIterator.handleREPRule(l, repProg, ruleIdx, minReps, maxReps, objNodeLists[ruleIdx], func(node nodeDesc) error {
			return t.sendObject(obj, encObj, node, &t.metaCollection)
		})
		if err != nil {
			if maxReplicas > 0 {
				return newMaxReplicasError(maxReplicas, maxReplicas-leftReplicas+stored, ruleIdx, overloaded, err)
			}
			return newCompletionError(err, stored > 0, overloaded)
		}

		if maxReplicas > 0 {
			if leftReplicas <= stored { // < should never happen
				break
			}
			leftReplicas -= stored
		}
	}

	if len(repRules) > 0 {
		err = t.submitMetaCollection(obj.Address(), &t.metaCollection)
		if err != nil {
			return err
		}
	}

	t.replicateRemainingPrimaryNodes(obj, objNodeLists[:len(fullRepRules)], fullRepRules, repProg)
	t.replicateRemainingECRules(obj, ecRules, objNodeLists[len(fullRepRules):], appliedECRules)

	return nil
}

func (t *distributedTarget) replicateRemainingPrimaryNodes(obj object.Object, nodeLists [][]netmap.NodeInfo, repRules []uint, prog *repProgress) {
	if t.initialPolicy == nil || t.postPlacementReplicator == nil || len(nodeLists) == 0 || prog == nil {
		return
	}

	remainingNodes := prog.remainingPrimaryNodes(nodeLists, repRules)
	if len(remainingNodes) == 0 {
		return
	}

	t.postPlacementReplicator.HandlePostPlacement(t.opCtx, &obj, remainingNodes)
}

func (t *distributedTarget) replicateRemainingECRules(obj object.Object, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo, applied []bool) {
	if t.initialPolicy == nil || t.postPlacementReplicator == nil || t.sessionSigner == nil || len(ecRules) == 0 {
		return
	}

	encodedByRule := make(map[iec.Rule][][]byte, len(ecRules))

	for ruleIdx := range ecRules {
		if applied[ruleIdx] {
			continue
		}

		payloadParts, ok := encodedByRule[ecRules[ruleIdx]]
		if !ok {
			var err error
			payloadParts, err = iec.Encode(ecRules[ruleIdx], obj.Payload())
			if err != nil {
				t.placementIterator.log.Info("failed to encode object for post-placement EC replication",
					zap.Stringer("object", obj.Address()),
					zap.Int("rule_idx", ruleIdx),
					zap.Stringer("rule", ecRules[ruleIdx]),
					zap.Error(err))
				continue
			}
			encodedByRule[ecRules[ruleIdx]] = payloadParts
		}

		totalParts := len(payloadParts)
		for partIdx := range payloadParts {
			partObj, err := formObjectForECPart(t.sessionSigner, obj, ruleIdx, partIdx, payloadParts)
			if err != nil {
				t.placementIterator.log.Info("failed to form EC part object for post-placement replication",
					zap.Stringer("object", obj.Address()),
					zap.Int("rule_idx", ruleIdx),
					zap.Int("part_idx", partIdx),
					zap.Error(err))
				continue
			}

			t.postPlacementReplicator.HandlePostPlacement(t.opCtx, &partObj, ecNodesForPart(nodeLists[ruleIdx], partIdx, totalParts))
		}
	}
}

func (t *distributedTarget) resetMetaCollection() {
	// this field is reused for sliced objects of the same container with
	// the same placement policy; placement's len must be kept the same, do
	// not nil the slice, keep it initialized
	for i := range t.metaCollection.signatures {
		t.metaCollection.signatures[i] = t.metaCollection.signatures[i][:0]
	}

	t.metaCollection.objectData = nil
}

func (t *distributedTarget) distributeObject(obj object.Object, encObj encodedObject,
	placementFn func(obj object.Object, encObj encodedObject) error) error {
	defer t.resetMetaCollection()

	if t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
		t.metaCollection.objectData = t.encodeObjectMetadata(obj)
	}

	return t.distributeObjectWithMeta(obj, encObj, &t.metaCollection, placementFn)
}

func (t *distributedTarget) distributeObjectWithMeta(obj object.Object, encObj encodedObject, metaC *metaCollection,
	placementFn func(obj object.Object, encObj encodedObject) error) error {
	id := obj.GetID()
	var err error
	if t.localOnly {
		var l = t.placementIterator.log.With(zap.Stringer("oid", id))

		err = t.writeObjectLocally(obj, encObj)
		if err != nil {
			err = fmt.Errorf("write object locally: %w", err)
			svcutil.LogServiceError(l, "PUT", nil, err)
		}
	} else {
		err = placementFn(obj, encObj)
	}
	if err != nil {
		return err
	}

	return t.submitMetaCollection(obj.Address(), metaC)
}

func (t *distributedTarget) submitMetaCollection(addr oid.Address, metaC *metaCollection) error {
	if t.localOnly || !t.localNodeInContainer || t.metainfoConsistencyAttr == "" {
		return nil
	}
	metaC.signaturesMtx.RLock()
	defer metaC.signaturesMtx.RUnlock()

	var await bool
	switch t.metainfoConsistencyAttr {
	// TODO: there was no constant in SDK at the code creation moment
	case "strict":
		await = true
	case "optimistic":
		await = false
	default:
		return nil
	}

	var objAccepted chan struct{}
	if await {
		objAccepted = make(chan struct{}, 1)
		t.metaSvc.NotifyObjectSuccess(objAccepted, addr)
	}

	err := t.cnrClient.SubmitObjectPut(metaC.objectData, metaC.signatures)
	if err != nil {
		if await {
			t.metaSvc.UnsubscribeFromObject(addr)
		}
		return fmt.Errorf("failed to submit %s object meta information: %w", addr, err)
	}

	if await {
		select {
		case <-t.opCtx.Done():
			t.metaSvc.UnsubscribeFromObject(addr)
			return fmt.Errorf("interrupted awaiting for %s object meta information: %w", addr, t.opCtx.Err())
		case <-objAccepted:
		}
	}

	t.placementIterator.log.Debug("submitted object meta information", zap.Stringer("addr", addr))

	return nil
}

func (t *distributedTarget) encodeObjectMetadata(obj object.Object) []byte {
	currBlock := t.fsState.CurrentBlock()
	currEpochDuration := t.fsState.CurrentEpochDuration()
	expectedVUB := (uint64(currBlock)/currEpochDuration + 2) * currEpochDuration

	firstObj := obj.GetFirstID()
	if obj.HasParent() && firstObj.IsZero() {
		// object itself is the first one
		firstObj = obj.GetID()
	}

	var deletedObjs []oid.ID
	var lockedObjs []oid.ID
	typ := obj.Type()
	switch typ {
	case object.TypeTombstone:
		deletedObjs = append(deletedObjs, obj.AssociatedObject())
	case object.TypeLock:
		lockedObjs = append(lockedObjs, obj.AssociatedObject())
	default:
	}

	return objectcore.EncodeReplicationMetaInfo(obj.GetContainerID(), obj.GetID(), firstObj, obj.GetPreviousID(),
		obj.PayloadSize(), typ, deletedObjs, lockedObjs, expectedVUB, t.networkMagicNumber)
}

func (t *distributedTarget) sendObject(obj object.Object, encObj encodedObject, node nodeDesc, metaC *metaCollection) error {
	if node.local {
		if err := t.writeObjectLocally(obj, encObj); err != nil {
			return fmt.Errorf("write object locally: %w", err)
		}

		if node.placementVector < 0 {
			// additional broadcast
			return nil
		}

		if t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
			sig, err := t.metaSigner.Sign(metaC.objectData)
			if err != nil {
				return fmt.Errorf("failed to sign object metadata: %w", err)
			}

			metaC.signaturesMtx.Lock()
			metaC.signatures[node.placementVector] = append(metaC.signatures[node.placementVector], sig)
			metaC.signaturesMtx.Unlock()
		}

		return nil
	}

	if t.relay != nil {
		return t.relay(node)
	}

	var sigsRaw []byte
	var err error
	if encObj.hdrOff > 0 {
		sigsRaw, err = t.transport.SendReplicationRequestToNode(t.opCtx, encObj.b, node.info)
		if err != nil {
			err = fmt.Errorf("replicate object to remote node (key=%x): %w", node.info.PublicKey(), err)
		}
	} else {
		err = putObjectToNode(t.opCtx, node.info, &obj, t.keyStorage, t.clientConstructor, t.commonPrm)
	}
	if err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}

	if t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
		if node.placementVector < 0 {
			// additional broadcast
			return nil
		}

		// These should technically be errors, but we don't have
		// a complete implementation now, so errors are substituted with logs.
		var l = t.placementIterator.log.With(zap.Stringer("oid", obj.GetID()),
			zap.Stringer("node", node.info.AddressGroup()))

		sigs, err := decodeSignatures(sigsRaw)
		if err != nil {
			return fmt.Errorf("failed to decode signatures: %w", err)
		}

		for i, sig := range sigs {
			if !bytes.Equal(sig.PublicKeyBytes(), node.info.PublicKey()) {
				l.Warn("public key differs in object meta signature", zap.Int("signature index", i))
				continue
			}

			if !sig.Verify(metaC.objectData) {
				continue
			}

			metaC.signaturesMtx.Lock()
			metaC.signatures[node.placementVector] = append(metaC.signatures[node.placementVector], sig.Value())
			metaC.signaturesMtx.Unlock()

			return nil
		}

		return errors.New("signatures were not found in object's metadata")
	}

	return nil
}

func (t *distributedTarget) writeObjectLocally(obj object.Object, encObj encodedObject) error {
	if err := putObjectLocally(t.localStorage, &obj, &encObj); err != nil {
		return err
	}

	return nil
}

func decodeSignatures(b []byte) ([]neofscrypto.Signature, error) {
	res := make([]neofscrypto.Signature, 3)
	for i := range res {
		var offset int
		var err error

		res[i], offset, err = decodeSignature(b)
		if err != nil {
			return nil, fmt.Errorf("decoding %d signature from proto message: %w", i, err)
		}

		b = b[offset:]
	}

	return res, nil
}

func decodeSignature(b []byte) (neofscrypto.Signature, int, error) {
	if len(b) < 4 {
		return neofscrypto.Signature{}, 0, fmt.Errorf("unexpected signature format: len: %d", len(b))
	}
	l := int(binary.LittleEndian.Uint32(b[:4]))
	if len(b) < 4+l {
		return neofscrypto.Signature{}, 0, fmt.Errorf("unexpected signature format: len: %d, len claimed: %d", len(b), l)
	}

	var res neofscrypto.Signature
	err := res.Unmarshal(b[4 : 4+l])
	if err != nil {
		return neofscrypto.Signature{}, 0, fmt.Errorf("invalid signature: %w", err)
	}

	return res, 4 + l, nil
}

type errNotEnoughNodes struct {
	listIndex int
	required  uint
	left      uint
}

func (x errNotEnoughNodes) Error() string {
	return fmt.Sprintf("number of replicas cannot be met for list #%d: %d required, %d nodes remaining",
		x.listIndex, x.required, x.left)
}

type placementIterator struct {
	log        *zap.Logger
	neoFSNet   NeoFSNetwork
	remotePool util.WorkerPool
}

type nodeResult struct {
	convertErr error
	desc       nodeDesc
	succeeded  bool
}

type nodeCounters struct {
	stored    uint
	processed uint
}

type repProgress struct {
	nodeResultsMtx    sync.RWMutex
	nodeResults       map[string]nodeResult
	nodesCounters     []nodeCounters
	nextNodeGroupKeys []string
	lastRespErr       atomic.Value
	wg                sync.WaitGroup
}

func newRepProgress(nodeLists [][]netmap.NodeInfo) *repProgress {
	return &repProgress{
		nodeResults:   make(map[string]nodeResult, islices.TwoDimSliceElementCount(nodeLists)),
		nodesCounters: make([]nodeCounters, len(nodeLists)),
	}
}

func (p *repProgress) remainingPrimaryNodes(nodeLists [][]netmap.NodeInfo, repRules []uint) []netmap.NodeInfo {
	var totalPrimaries int
	for i := range nodeLists {
		totalPrimaries += primaryNodesCount(nodeLists[i], repRules, i)
	}

	remainingNodes := make([]netmap.NodeInfo, 0, totalPrimaries)
	seen := make(map[string]struct{}, totalPrimaries)

	p.nodeResultsMtx.RLock()
	defer p.nodeResultsMtx.RUnlock()

	for i := range nodeLists {
		for j := range primaryNodesCount(nodeLists[i], repRules, i) {
			node := nodeLists[i][j]
			pk := string(node.PublicKey())
			if _, ok := seen[pk]; ok {
				continue
			}
			seen[pk] = struct{}{}

			res, ok := p.nodeResults[pk]
			if ok && res.succeeded {
				continue
			}

			remainingNodes = append(remainingNodes, node)
		}
	}

	return remainingNodes
}

func primaryNodesCount(nodes []netmap.NodeInfo, repRules []uint, i int) int {
	if i >= len(repRules) || int(repRules[i]) >= len(nodes) {
		return len(nodes)
	}

	return int(repRules[i])
}

func repToNode(l *zap.Logger, prog *repProgress, pubKeyStr string, listInd int, nr nodeResult, f func(nodeDesc) error) {
	nr.desc.placementVector = listInd
	err := f(nr.desc)
	prog.nodeResultsMtx.Lock()
	if listInd >= 0 {
		if nr.succeeded = err == nil; nr.succeeded {
			prog.nodesCounters[listInd].stored++
		}
	}
	prog.nodeResults[pubKeyStr] = nr
	prog.nodeResultsMtx.Unlock()
	if err != nil {
		prog.lastRespErr.Store(err)
		if listInd >= 0 {
			svcutil.LogServiceError(l, "PUT", nr.desc.info.AddressGroup(), err)
		} else {
			svcutil.LogServiceError(l, "PUT (extra broadcast)", nr.desc.info.AddressGroup(), err)
		}
		return
	}
}

func (x placementIterator) handleREPRule(l *zap.Logger, prog *repProgress, listInd int, minReps, maxReps uint, nodeList []netmap.NodeInfo, f func(desc nodeDesc) error) (uint, bool, error) {
	var overloaded bool

	for {
		if prog.nodesCounters[listInd].stored >= maxReps { // > should never happen
			return prog.nodesCounters[listInd].stored, false, nil
		}

		var minRequired uint
		if minReps > prog.nodesCounters[listInd].stored {
			minRequired = minReps - prog.nodesCounters[listInd].stored
		}

		listLen := uint(len(nodeList))
		if listLen-prog.nodesCounters[listInd].processed < minRequired {
			var err error = errNotEnoughNodes{listIndex: listInd, required: minRequired, left: listLen - prog.nodesCounters[listInd].processed}
			if e, _ := prog.lastRespErr.Load().(error); e != nil {
				err = fmt.Errorf("%w (last node error: %w)", err, e)
			}
			return prog.nodesCounters[listInd].stored, overloaded, errIncompletePut{singleErr: err}
		} else if prog.nodesCounters[listInd].processed >= listLen { // > should never happen
			// The required minimum is reached, and the maximum is unreachable.
			return prog.nodesCounters[listInd].stored, false, nil
		}

		replRem := maxReps - prog.nodesCounters[listInd].stored // overflow prevented above

		prog.nextNodeGroupKeys = slices.Grow(prog.nextNodeGroupKeys, int(replRem))[:0]
		for ; prog.nodesCounters[listInd].processed < listLen && uint(len(prog.nextNodeGroupKeys)) < replRem; prog.nodesCounters[listInd].processed++ {
			j := prog.nodesCounters[listInd].processed
			pk := nodeList[j].PublicKey()
			pks := string(pk)
			prog.nodeResultsMtx.RLock()
			nr, ok := prog.nodeResults[pks]
			prog.nodeResultsMtx.RUnlock()
			if ok {
				if nr.succeeded { // in some previous list
					prog.nodesCounters[listInd].stored++
					replRem--
				}
				continue
			}
			if nr.desc.local = x.neoFSNet.IsLocalNodePublicKey(pk); !nr.desc.local {
				nr.desc.info, nr.convertErr = convertNodeInfo(nodeList[j])
			}
			prog.nodeResultsMtx.Lock()
			prog.nodeResults[pks] = nr
			prog.nodeResultsMtx.Unlock()
			if nr.convertErr == nil {
				prog.nextNodeGroupKeys = append(prog.nextNodeGroupKeys, pks)
				continue
			}
			// critical error that may ultimately block the storage service. Normally it
			// should not appear because entry into the network map under strict control
			l.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
				zap.String("public key", netmap.StringifyPublicKey(nodeList[j])), zap.Error(nr.convertErr))
			if minReps > prog.nodesCounters[listInd].stored {
				minRequired = minReps - prog.nodesCounters[listInd].stored
			}
			if listLen-prog.nodesCounters[listInd].processed-1 < minRequired { // -1 includes current node failure
				err := fmt.Errorf("%w (last node error: failed to decode network addresses: %w)",
					errNotEnoughNodes{listIndex: listInd, required: minRequired, left: listLen - prog.nodesCounters[listInd].processed - 1},
					nr.convertErr)
				return prog.nodesCounters[listInd].stored, false, errIncompletePut{singleErr: err}
			}
			// continue to try the best to save required number of replicas
		}
		for j := range prog.nextNodeGroupKeys {
			pks := prog.nextNodeGroupKeys[j]
			prog.nodeResultsMtx.RLock()
			nr := prog.nodeResults[pks]
			prog.nodeResultsMtx.RUnlock()
			if nr.desc.local {
				prog.wg.Go(func() { repToNode(l, prog, pks, listInd, nr, f) })
				continue
			}
			prog.wg.Add(1)
			if err := x.remotePool.Submit(func() {
				repToNode(l, prog, pks, listInd, nr, f)
				prog.wg.Done()
			}); err != nil {
				prog.wg.Done()
				if errors.Is(err, ants.ErrPoolOverload) {
					overloaded = true
				}
				err = fmt.Errorf("submit next job to save an object to the worker pool: %w", err)
				svcutil.LogWorkerPoolError(l, "PUT", err)
			}
		}
		prog.wg.Wait()
	}
}

func (x placementIterator) iterateNodesForObject(obj oid.ID, replCounts []uint, nodeLists [][]netmap.NodeInfo, broadcast bool, f func(nodeDesc) error) error {
	var l = x.log.With(zap.Stringer("oid", obj))

	prog := newRepProgress(nodeLists)

	// TODO: processing node lists in ascending size can potentially reduce failure
	//  latency and volume of "unfinished" data to be garbage-collected. Also after
	//  the failure of any of the nodes the ability to comply with the policy
	//  requirements may be lost.
	for i := range replCounts {
		stored, overloaded, err := x.handleREPRule(l, prog, i, replCounts[i], replCounts[i], nodeLists[i], f)
		if err != nil {
			return newCompletionError(err, stored > 0, overloaded)
		}
	}
	if !broadcast {
		return nil
	}
	// TODO: since main part of the operation has already been completed, and
	//  additional broadcast does not affect the result, server should immediately
	//  send the response
broadcast:
	for i := range nodeLists {
		for j := range nodeLists[i] {
			pk := nodeLists[i][j].PublicKey()
			pks := string(pk)
			prog.nodeResultsMtx.RLock()
			nr, ok := prog.nodeResults[pks]
			prog.nodeResultsMtx.RUnlock()
			if ok {
				continue
			}
			if nr.desc.local = x.neoFSNet.IsLocalNodePublicKey(pk); !nr.desc.local {
				nr.desc.info, nr.convertErr = convertNodeInfo(nodeLists[i][j])
			}
			prog.nodeResultsMtx.Lock()
			prog.nodeResults[pks] = nr
			prog.nodeResultsMtx.Unlock()
			if nr.convertErr != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control
				l.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(nr.convertErr))
				continue // to send as many replicas as possible
			}
			if nr.desc.local {
				prog.wg.Go(func() { repToNode(l, prog, pks, -1, nr, f) })
				continue
			}
			prog.wg.Add(1)
			if err := x.remotePool.Submit(func() {
				repToNode(l, prog, pks, -1, nr, f)
				prog.wg.Done()
			}); err != nil {
				prog.wg.Done()
				svcutil.LogWorkerPoolError(l, "PUT (extra broadcast)", err)
				break broadcast
			}
		}
	}
	prog.wg.Wait()
	return nil
}

func convertNodeInfo(nodeInfo netmap.NodeInfo) (client.NodeInfo, error) {
	var res client.NodeInfo
	var endpoints network.AddressGroup
	if err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeInfo)); err != nil {
		return res, err
	}
	res.SetAddressGroup(endpoints)
	res.SetPublicKey(nodeInfo.PublicKey())
	return res, nil
}
