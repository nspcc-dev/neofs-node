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
	// Undefined when policy has no EC rules.
	ecPart iec.PartInfo

	initialPlacementPolicy *InitialPlacementPolicy
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
	typ := obj.Type()

	initialPUT := t.sessionSigner != nil || !t.localOnly
	if initialPUT && typ == object.TypeRegular && t.initialPlacementPolicy != nil {
		if t.sessionSigner != nil || len(ecRules) == 0 {
			err := t.handleInitialPlacementPolicy(*t.initialPlacementPolicy, repRules, ecRules, objNodeLists, obj, encObj)
			if err != nil {
				return fmt.Errorf("initial placement failed: %w", err)
			}
			return nil
		} else if t.ecPart.RuleIndex < 0 { // prevent EC encoding
			err := t.handleInitialPlacementPolicy(InitialPlacementPolicy{
				MaxReplicas:        t.initialPlacementPolicy.MaxReplicas,
				PreferLocal:        t.initialPlacementPolicy.PreferLocal,
				MaxPerRuleReplicas: t.initialPlacementPolicy.MaxPerRuleReplicas[:len(repRules)],
			}, repRules, nil, objNodeLists[:len(repRules)], obj, encObj)
			if err != nil {
				return fmt.Errorf("initial placement failed: %w", err)
			}
			return nil
		} // initial policy does not apply to individual EC parts
	}

	if len(repRules) > 0 || typ == object.TypeTombstone || typ == object.TypeLock || typ == object.TypeLink {
		broadcast := typ == object.TypeTombstone || typ == object.TypeLink || (!t.localOnly && typ == object.TypeLock) || len(obj.Children()) > 0

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

	if len(ecRules) > 0 {
		if t.ecPart.RuleIndex >= 0 { // already encoded EC part
			total := int(ecRules[t.ecPart.RuleIndex].DataPartNum + ecRules[t.ecPart.RuleIndex].ParityPartNum)
			nodes := objNodeLists[len(repRules)+t.ecPart.RuleIndex]
			return t.saveECPart(obj, encObj, t.ecPart.RuleIndex, t.ecPart.Index, total, nodes, &t.metaCollection)
		}

		if t.sessionSigner != nil {
			if err := t.ecAndSaveObject(t.sessionSigner, obj, ecRules, objNodeLists[len(repRules):]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *distributedTarget) distributeObject(obj object.Object, encObj encodedObject,
	placementFn func(obj object.Object, encObj encodedObject) error) error {
	defer func() {
		// this field is reused for sliced objects of the same container with
		// the same placement policy; placement's len must be kept the same, do
		// not nil the slice, keep it initialized
		for i := range t.metaCollection.signatures {
			t.metaCollection.signatures[i] = t.metaCollection.signatures[i][:0]
		}
	}()

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

		addr := obj.Address()
		var objAccepted chan struct{}
		if await {
			objAccepted = make(chan struct{}, 1)
			t.metaSvc.NotifyObjectSuccess(objAccepted, addr)
		}

		err = t.cnrClient.SubmitObjectPut(metaC.objectData, metaC.signatures)
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
	}

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

	processNode := func(pubKeyStr string, listInd int, nr nodeResult, wg *sync.WaitGroup) {
		defer wg.Done()
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
					var busy = new(apistatus.Busy)
					busy.SetMessage(retErr.Error())
					return busy
				}
				return wrapIncompleteError(retErr)
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
				logNodeConversionError(l, nodeLists[listInd][j], nr.convertErr)
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
				wg.Add(1)
				if nr.desc.local {
					go processNode(pks, listInd, nr, &wg)
					continue
				}
				if err := x.remotePool.Submit(func() {
					processNode(pks, listInd, nr, &wg)
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
				logNodeConversionError(l, nodeLists[i][j], nr.convertErr)
				continue // to send as many replicas as possible
			}
			wg.Add(1)
			if nr.desc.local {
				go processNode(pks, -1, nr, &wg)
				continue
			}
			if err := x.remotePool.Submit(func() {
				processNode(pks, -1, nr, &wg)
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
