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

	initial := t.initialPolicy != nil && (t.sessionSigner != nil || !t.localOnly)

	if t.ecPart.RuleIndex >= 0 { // already encoded EC part
		// part info should already be verified, so we don't prevent out-of-range panic here
		if initial && len(t.initialPolicy.ReplicaLimits()) != 0 && t.initialPolicy.ReplicaLimits()[len(repRules)+t.ecPart.RuleIndex] == 0 {
			// clients can encode data themselves, but they're unlikely to enforce the initial placement policy. So, let this not be an error
			return nil
		}

		total := int(ecRules[t.ecPart.RuleIndex].DataPartNum + ecRules[t.ecPart.RuleIndex].ParityPartNum)
		nodes := objNodeLists[len(repRules)+t.ecPart.RuleIndex]
		return t.saveECPart(obj, encObj, t.ecPart.RuleIndex, t.ecPart.Index, total, nodes, &t.metaCollection)
	}

	var ecLimits []uint32

	if initial {
		if initialLimits := t.initialPolicy.ReplicaLimits(); len(initialLimits) > 0 {
			if len(initialLimits) != len(repRules)+len(ecRules) { // required by policy, but better to double-check
				return fmt.Errorf("ReplicaLimits has len %d while main policy has %d REP and %d EC rules", len(initialLimits), len(repRules), len(ecRules))
			}
			// TODO: make ContainerNodes.PrimaryCounts() to return []uint32, and just assign
			repRules = make([]uint, len(repRules)) // recreate to not mutate cache
			for i := range repRules {
				repRules[i] = uint(initialLimits[i])
			}
			ecLimits = initialLimits[len(repRules):]
		}

		maxReplicas := t.initialPolicy.MaxReplicas()
		if maxReplicas == 0 && t.placementIterator.linearReplNum > 0 {
			maxReplicas = uint32(t.placementIterator.linearReplNum)
		}
		if maxReplicas > 0 {
			return t.putMaxReplicas(obj, encObj, ecRules, objNodeLists, repRules, ecLimits, maxReplicas, t.initialPolicy.PreferLocal())
		}
	}

	if len(repRules) > 0 {
		return t.distributeObject(obj, encObj, func(obj object.Object, encObj encodedObject) error {
			return t.placementIterator.iterateNodesForObject(obj.GetID(), repRules, objNodeLists, false, func(node nodeDesc) error {
				return t.sendObject(obj, encObj, node, &t.metaCollection)
			})
		})
	}

	if len(ecRules) > 0 && t.sessionSigner != nil {
		if err := t.ecAndSaveObject(t.sessionSigner, obj, ecRules, objNodeLists[len(repRules):], ecLimits); err != nil {
			return err
		}
	}

	return nil
}

func (t *distributedTarget) resetMetaCollection() {
	// this field is reused for sliced objects of the same container with
	// the same placement policy; placement's len must be kept the same, do
	// not nil the slice, keep it initialized
	for i := range t.metaCollection.signatures {
		t.metaCollection.signatures[i] = t.metaCollection.signatures[i][:0]
	}
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

	if !t.localOnly && t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
		return t.submitCollectedMeta(obj.Address(), metaC)
	}

	return nil
}
func (t *distributedTarget) submitCollectedMeta(addr oid.Address, metaC *metaCollection) error {
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
	/* request-dependent */
	// when non-zero, this setting simplifies the object's storage policy
	// requirements to a fixed number of object replicas to be retained
	linearReplNum uint
}

func (x placementIterator) iterateNodesForObject(obj oid.ID, replCounts []uint, nodeLists [][]netmap.NodeInfo, broadcast bool, f func(nodeDesc) error) error {
	var l = x.log.With(zap.Stringer("oid", obj))
	if x.linearReplNum > 0 {
		ns := slices.Concat(nodeLists...)
		nodeLists = [][]netmap.NodeInfo{ns}
		replCounts = []uint{x.linearReplNum}
	}
	var processedNodesMtx sync.RWMutex
	var nextNodeGroupKeys []string
	var wg sync.WaitGroup
	var lastRespErr atomic.Value
	nodesCounters := make([]struct{ stored, processed uint }, len(nodeLists))
	type nodeResult struct {
		convertErr error
		desc       nodeDesc
		succeeded  bool
	}
	var nrCap int
	for i := range nodeLists {
		nrCap += len(nodeLists[i])
	}
	nodeResults := make(map[string]nodeResult, nrCap)

	processNode := func(pubKeyStr string, listInd int, nr nodeResult) {
		nr.desc.placementVector = listInd
		err := f(nr.desc)
		processedNodesMtx.Lock()
		if listInd >= 0 {
			if nr.succeeded = err == nil; nr.succeeded {
				nodesCounters[listInd].stored++
			}
		}
		nodeResults[pubKeyStr] = nr
		processedNodesMtx.Unlock()
		if err != nil {
			lastRespErr.Store(err)
			if listInd >= 0 {
				svcutil.LogServiceError(l, "PUT", nr.desc.info.AddressGroup(), err)
			} else {
				svcutil.LogServiceError(l, "PUT (extra broadcast)", nr.desc.info.AddressGroup(), err)
			}
			return
		}
	}

	// TODO: processing node lists in ascending size can potentially reduce failure
	//  latency and volume of "unfinished" data to be garbage-collected. Also after
	//  the failure of any of the nodes the ability to comply with the policy
	//  requirements may be lost.
	for i := range replCounts {
		var (
			listInd    = i
			overloaded bool
		)
		for {
			replRem := replCounts[listInd] - nodesCounters[listInd].stored
			if replRem == 0 {
				break
			}
			listLen := uint(len(nodeLists[listInd]))
			if listLen-nodesCounters[listInd].processed < replRem {
				var err error = errNotEnoughNodes{listIndex: listInd, required: replRem, left: listLen - nodesCounters[listInd].processed}
				if e, _ := lastRespErr.Load().(error); e != nil {
					err = fmt.Errorf("%w (last node error: %w)", err, e)
				}
				var retErr = errIncompletePut{singleErr: err}
				if nodesCounters[listInd].stored == 0 {
					if !overloaded {
						return retErr
					}
					return newBusyError(retErr)
				}
				return newIncompleteError(retErr)
			}
			nextNodeGroupKeys = slices.Grow(nextNodeGroupKeys, int(replRem))[:0]
			for ; nodesCounters[listInd].processed < listLen && uint(len(nextNodeGroupKeys)) < replRem; nodesCounters[listInd].processed++ {
				j := nodesCounters[listInd].processed
				pk := nodeLists[listInd][j].PublicKey()
				pks := string(pk)
				processedNodesMtx.RLock()
				nr, ok := nodeResults[pks]
				processedNodesMtx.RUnlock()
				if ok {
					if nr.succeeded { // in some previous list
						nodesCounters[listInd].stored++
						replRem--
					}
					continue
				}
				if nr.desc.local = x.neoFSNet.IsLocalNodePublicKey(pk); !nr.desc.local {
					nr.desc.info, nr.convertErr = convertNodeInfo(nodeLists[listInd][j])
				}
				processedNodesMtx.Lock()
				nodeResults[pks] = nr
				processedNodesMtx.Unlock()
				if nr.convertErr == nil {
					nextNodeGroupKeys = append(nextNodeGroupKeys, pks)
					continue
				}
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control
				l.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[listInd][j])), zap.Error(nr.convertErr))
				if listLen-nodesCounters[listInd].processed-1 < replRem { // -1 includes current node failure
					err := fmt.Errorf("%w (last node error: failed to decode network addresses: %w)",
						errNotEnoughNodes{listIndex: listInd, required: replRem, left: listLen - nodesCounters[listInd].processed - 1},
						nr.convertErr)
					return errIncompletePut{singleErr: err}
				}
				// continue to try the best to save required number of replicas
			}
			for j := range nextNodeGroupKeys {
				pks := nextNodeGroupKeys[j]
				processedNodesMtx.RLock()
				nr := nodeResults[pks]
				processedNodesMtx.RUnlock()
				if nr.desc.local {
					wg.Go(func() { processNode(pks, listInd, nr) })
					continue
				}
				wg.Add(1)
				if err := x.remotePool.Submit(func() {
					processNode(pks, listInd, nr)
					wg.Done()
				}); err != nil {
					wg.Done()
					if errors.Is(err, ants.ErrPoolOverload) {
						overloaded = true
					}
					err = fmt.Errorf("submit next job to save an object to the worker pool: %w", err)
					svcutil.LogWorkerPoolError(l, "PUT", err)
				}
			}
			wg.Wait()
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
			processedNodesMtx.RLock()
			nr, ok := nodeResults[pks]
			processedNodesMtx.RUnlock()
			if ok {
				continue
			}
			if nr.desc.local = x.neoFSNet.IsLocalNodePublicKey(pk); !nr.desc.local {
				nr.desc.info, nr.convertErr = convertNodeInfo(nodeLists[i][j])
			}
			processedNodesMtx.Lock()
			nodeResults[pks] = nr
			processedNodesMtx.Unlock()
			if nr.convertErr != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control
				l.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(nr.convertErr))
				continue // to send as many replicas as possible
			}
			if nr.desc.local {
				wg.Go(func() { processNode(pks, -1, nr) })
				continue
			}
			wg.Add(1)
			if err := x.remotePool.Submit(func() {
				processNode(pks, -1, nr)
				wg.Done()
			}); err != nil {
				wg.Done()
				svcutil.LogWorkerPoolError(l, "PUT (extra broadcast)", err)
				break broadcast
			}
		}
	}
	wg.Wait()
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

type ruleCounters struct {
	stored    atomic.Uint32
	processed uint
}

type maxReplicasError struct {
	needed uint32
	ok     uint32
	left   uint32
}

func newMaxReplicasError(needed uint32, ok uint32, undone uint32) maxReplicasError {
	return maxReplicasError{needed: needed, ok: ok, left: undone}
}

func (x maxReplicasError) Error() string {
	return fmt.Sprintf("unable to reach MaxReplicas %d (succeeded: %d, left nodes: %d)", x.needed, x.ok, x.left)
}

func (t *distributedTarget) putMaxReplicas(obj object.Object, encObj encodedObject, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	repLimits []uint, ecLimits []uint32, maxReplicas uint32, preferLocal bool) (err error) {
	if t.localNodeInContainer && t.metainfoConsistencyAttr != "" {
		t.metaCollection.objectData = t.encodeObjectMetadata(obj)
		defer func() {
			if err == nil {
				err = t.submitCollectedMeta(obj.Address(), &t.metaCollection)
			}
			t.resetMetaCollection()
		}()
	}

	var poolErr error
	var wg sync.WaitGroup
	var repResultsMtx sync.RWMutex
	repResults := make(map[string]bool, islices.TwoDimSliceElementCount(nodeLists))
	rulesCounters := make([]ruleCounters, len(nodeLists))

	leftReplicas := maxReplicas
	for {
		preferLocal, poolErr = t.fillNextMaxReplicasGroup(obj, encObj, ecRules, nodeLists, repLimits, ecLimits, leftReplicas, preferLocal,
			&wg, rulesCounters, &repResultsMtx, repResults)
		if poolErr != nil {
			t.placementIterator.log.Error("could not push task to worker pool",
				zap.String("request", "PUT"), zap.Stringer("oid", obj.GetID()), zap.Error(poolErr))
			if !errors.Is(poolErr, ants.ErrPoolOverload) {
				return fmt.Errorf("unknown worker pool error: %w", err)
			}
		}

		wg.Wait()

		ok, undone := calculateMaxReplicasProgress(maxReplicas, ecRules, nodeLists, repLimits, ecLimits, rulesCounters)
		if ok >= maxReplicas {
			return nil
		}

		if maxReplicas > ok+undone {
			err = newMaxReplicasError(maxReplicas, ok, undone)
			if ok > 0 {
				err = newIncompleteError(err)
			}
			if poolErr != nil {
				return newBusyError(err)
			}
			return err
		}

		leftReplicas = maxReplicas - ok
	}
}

func (t *distributedTarget) fillNextMaxReplicasGroup(obj object.Object, encObj encodedObject, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	repLimits []uint, ecLimits []uint32, limit uint32, localOnly bool, wg *sync.WaitGroup, rulesCounters []ruleCounters, repResultsMtx *sync.RWMutex, repResults map[string]bool) (bool, error) {
	for {
		var added bool
		for i := range repLimits {
			ruleCounter := &rulesCounters[i]

			if repLimits[i] == 0 || // disabled in policy
				ruleCounter.processed == uint(len(nodeLists[i])) || // this rule can no longer be followed, but the goal can still be achieved
				uint(ruleCounter.stored.Load()) == repLimits[i] || // do not overflow the rule limit
				localOnly && !localNodeInSet(t.placementIterator.neoFSNet, nodeLists[i]) { // we're still trying with local sets
				continue
			}

			node := nodeLists[i][ruleCounter.processed]
			ruleCounter.processed++

			pubKey := node.PublicKey()
			pubKeyStr := string(pubKey)

			repResultsMtx.RLock()
			succeeded, ok := repResults[pubKeyStr]
			repResultsMtx.RUnlock()
			if !ok {
				for j := range repLimits {
					if j != i && slices.ContainsFunc(nodeLists[j][:rulesCounters[j].processed], func(node netmap.NodeInfo) bool {
						return bytes.Equal(node.PublicKey(), pubKey)
					}) {
						ok = true
						break
					}
				}
			}
			if ok { // node appears in other set and has already been handled
				if succeeded {
					ruleCounter.stored.Add(1)
					if limit--; limit == 0 {
						return localOnly, nil
					}
				}
				added = true
				continue
			}

			if t.placementIterator.neoFSNet.IsLocalNodePublicKey(pubKey) {
				wg.Go(func() {
					err := t.sendObject(obj, encObj, nodeDesc{local: true, placementVector: i}, &t.metaCollection)
					if err != nil {
						repResultsMtx.Lock()
						repResults[pubKeyStr] = false
						repResultsMtx.Unlock()
						t.placementIterator.log.Error("local object PUT failed", zap.Stringer("oid", obj.GetID()), zap.Error(err))
						return
					}

					repResultsMtx.Lock()
					repResults[pubKeyStr] = false
					repResultsMtx.Unlock()
					ruleCounter.stored.Add(1)
				})

				if limit--; limit == 0 {
					return localOnly, nil
				}
				added = true
				continue
			}

			ni, err := convertNodeInfo(node)
			if err != nil {
				// same as for main policy handler. This will likely go away with https://github.com/nspcc-dev/neofs-node/issues/3565
				t.placementIterator.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.Stringer("oid", obj.GetID()), zap.String("public key", netmap.StringifyPublicKey(node)), zap.Error(err))
				continue
			}

			wg.Add(1)
			err = t.placementIterator.remotePool.Submit(func() {
				defer wg.Done()

				err := t.sendObject(obj, encObj, nodeDesc{placementVector: i, info: ni}, &t.metaCollection)
				if err != nil {
					repResultsMtx.Lock()
					repResults[pubKeyStr] = false
					repResultsMtx.Unlock()
					t.placementIterator.log.Error("remote object PUT failed", zap.Stringer("oid", obj.GetID()), zap.Error(err))
					return
				}

				repResultsMtx.Lock()
				repResults[pubKeyStr] = false
				repResultsMtx.Unlock()
				ruleCounter.stored.Add(1)
			})
			if err != nil {
				wg.Done()
				return localOnly, err
			}

			if limit--; limit == 0 {
				return localOnly, nil
			}
			added = true
		}

		for i := range ecRules {
			ruleCounter := &rulesCounters[i]

			if ecLimits != nil && ecLimits[i] == 0 || // disabled in policy
				ruleCounter.processed > 0 || // partitioning failed, but the goal can still be achieved
				ruleCounter.stored.Load() > 0 || // already partitioned
				localOnly && !localNodeInSet(t.placementIterator.neoFSNet, nodeLists[i]) { // we're still trying with local sets
				continue
			}

			wg.Add(1)
			err := t.placementIterator.remotePool.Submit(func() {
				defer wg.Done()
				_, err := t.ecObjectAndSaveParts(t.sessionSigner, obj, ecRules[i], i, nodeLists[len(repLimits)+i])
				if err != nil {
					t.placementIterator.log.Error("EC PUT failed", zap.Stringer("oid", obj.GetID()), zap.Error(err))
					return
				}
				ruleCounter.stored.Store(1) // no concurrency
			})
			if err != nil {
				wg.Done()
				return localOnly, err
			}

			if limit--; limit == 0 {
				return localOnly, nil
			}
			added = true
		}

		if !added {
			if !localOnly {
				return false, nil
			}
			localOnly = false
		}
	}
}

func calculateMaxReplicasProgress(maxReplicas uint32, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo, repLimits []uint, ecLimits []uint32, rulesCounters []ruleCounters) (uint32, uint32) {
	var ok, undone uint32

	for i := range repLimits {
		if repLimits[i] == 0 {
			continue
		}
		if ok += rulesCounters[i].stored.Load(); ok >= maxReplicas {
			return ok, undone
		}
		if uint(len(nodeLists[i])) > rulesCounters[i].processed {
			undone += uint32(len(nodeLists[i])) - uint32(rulesCounters[i].processed)
		}
	}

	for i := range ecRules {
		if ecLimits != nil && ecLimits[i] == 0 {
			continue
		}
		if rulesCounters[i].stored.Load() > 0 {
			if ok++; ok >= maxReplicas {
				return ok, undone
			}
		}
		if rulesCounters[i].processed > 0 {
			undone++
		}
	}

	return ok, undone
}
