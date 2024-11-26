package putsvc

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type preparedObjectTarget interface {
	WriteObject(*objectSDK.Object, object.ContentMeta, encodedObject) error
	Close() (oid.ID, *neofscrypto.Signature, error)
}

type distributedTarget struct {
	placementIterator placementIterator

	obj                *objectSDK.Object
	objMeta            object.ContentMeta
	networkMagicNumber uint32
	objSharedMeta      []byte

	localNodeInContainer bool
	localNodeSigner      neofscrypto.Signer
	// - object if localOnly
	// - replicate request if localNodeInContainer
	// - payload otherwise
	encodedObject encodedObject

	nodeTargetInitializer func(nodeDesc) preparedObjectTarget

	relay func(nodeDesc) error

	fmt *object.FormatValidator
}

type nodeDesc struct {
	local bool

	info client.NodeInfo
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

func (t *distributedTarget) WriteHeader(hdr *objectSDK.Object) error {
	payloadLen := hdr.PayloadSize()
	if payloadLen > math.MaxInt {
		return fmt.Errorf("too big payload of physically stored for this server %d > %d", payloadLen, math.MaxInt)
	}

	if t.localNodeInContainer {
		var err error
		if t.placementIterator.localOnly {
			t.encodedObject, err = encodeObjectWithoutPayload(*hdr, int(payloadLen))
		} else {
			t.encodedObject, err = encodeReplicateRequestWithoutPayload(t.localNodeSigner, *hdr, int(payloadLen), true)
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

	tombOrLink := t.obj.Type() == objectSDK.TypeLink || t.obj.Type() == objectSDK.TypeTombstone

	if !t.placementIterator.broadcast && len(t.obj.Children()) > 0 || tombOrLink {
		// enabling extra broadcast for linking and tomb objects
		t.placementIterator.broadcast = true
	}

	// v2 split link object and tombstone validations are expensive routines
	// and are useless if the node does not belong to the container, since
	// another node is responsible for the validation and may decline it,
	// does not matter what this node thinks about it
	if !tombOrLink || t.localNodeInContainer {
		var err error
		if t.objMeta, err = t.fmt.ValidateContent(t.obj); err != nil {
			return oid.ID{}, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
		}
	}

	var deletedObjs []oid.ID
	var lockedObjs []oid.ID
	switch t.objMeta.Type() {
	case objectSDK.TypeTombstone:
		deletedObjs = t.objMeta.Objects()
	case objectSDK.TypeLock:
		lockedObjs = t.objMeta.Objects()
	default:
	}

	t.objSharedMeta = object.EncodeReplicationMetaInfo(t.obj.GetContainerID(), t.obj.GetID(), t.obj.PayloadSize(), deletedObjs,
		lockedObjs, t.obj.CreationEpoch(), t.networkMagicNumber)
	id := t.obj.GetID()
	return id, t.placementIterator.iterateNodesForObject(id, t.sendObject)
}

func (t *distributedTarget) sendObject(node nodeDesc) error {
	if !node.local && t.relay != nil {
		return t.relay(node)
	}

	target := t.nodeTargetInitializer(node)

	err := target.WriteObject(t.obj, t.objMeta, t.encodedObject)
	if err != nil {
		return fmt.Errorf("could not write header: %w", err)
	}

	_, sig, err := target.Close()
	if err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}

	if t.localNodeInContainer && !node.local {
		// These should technically be errors, but we don't have
		// a complete implementation now, so errors are substituted with logs.
		var l = t.placementIterator.log.With(zap.Stringer("oid", t.obj.GetID()))

		if sig == nil {
			l.Info("missing object meta signature")
			return nil
		}

		if !bytes.Equal(sig.PublicKeyBytes(), node.info.PublicKey()) {
			l.Info("public key differs in object meta signature")
			return nil
		}

		if !sig.Verify(t.objSharedMeta) {
			l.Info("meta signature verification failed", zap.String("node", network.StringifyGroup(node.info.AddressGroup())))
		}
	}

	return nil
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
	localPool  util.WorkerPool
	remotePool util.WorkerPool
	/* request-dependent */
	containerNodes ContainerNodes
	localOnly      bool
	localNodePos   [2]int // in containerNodeSets. Undefined localOnly is false
	// when non-zero, this setting simplifies the object's storage policy
	// requirements to a fixed number of object replicas to be retained
	linearReplNum uint
	// whether to perform additional best-effort of sending the object replica to
	// all reserve nodes of the container
	broadcast bool
}

func (x placementIterator) iterateNodesForObject(obj oid.ID, f func(nodeDesc) error) error {
	var err error
	var nodeLists [][]netmap.NodeInfo
	var replCounts []uint
	var l = x.log.With(zap.Stringer("oid", obj))
	if x.localOnly {
		// TODO: although this particular case fits correctly into the general approach,
		//  much less actions can be done
		nn := x.containerNodes.Unsorted()
		nodeLists = [][]netmap.NodeInfo{{nn[x.localNodePos[0]][x.localNodePos[1]]}}
		replCounts = []uint{1}
	} else {
		if nodeLists, err = x.containerNodes.SortForObject(obj); err != nil {
			return fmt.Errorf("sort container nodes for the object: %w", err)
		}
		if x.linearReplNum > 0 {
			ns := slices.Concat(nodeLists...)
			nodeLists = [][]netmap.NodeInfo{ns}
			replCounts = []uint{x.linearReplNum}
		} else {
			replCounts = x.containerNodes.PrimaryCounts()
		}
	}
	var processedNodesMtx sync.RWMutex
	var nextNodeGroupKeys []string
	var wg sync.WaitGroup
	var lastRespErr atomic.Value
	nodesCounters := make([]struct{ stored, processed uint }, len(nodeLists))
	nodeResults := make(map[string]struct {
		convertErr error
		desc       nodeDesc
		succeeded  bool
	})
	// TODO: processing node lists in ascending size can potentially reduce failure
	//  latency and volume of "unfinished" data to be garbage-collected. Also after
	//  the failure of any of the nodes the ability to comply with the policy
	//  requirements may be lost.
	for i := range nodeLists {
		listInd := i
		for {
			replRem := replCounts[listInd] - nodesCounters[listInd].stored
			if replRem == 0 {
				break
			}
			listLen := uint(len(nodeLists[listInd]))
			if listLen-nodesCounters[listInd].processed < replRem {
				err = errNotEnoughNodes{listIndex: listInd, required: replRem, left: listLen - nodesCounters[listInd].processed}
				if e, _ := lastRespErr.Load().(error); e != nil {
					err = fmt.Errorf("%w (last node error: %w)", err, e)
				}
				return errIncompletePut{singleErr: err}
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
					nr.desc.info, nr.convertErr = x.convertNodeInfo(nodeLists[listInd][j])
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
					err = fmt.Errorf("%w (last node error: failed to decode network addresses: %w)",
						errNotEnoughNodes{listIndex: listInd, required: replRem, left: listLen - nodesCounters[listInd].processed - 1},
						nr.convertErr)
					return errIncompletePut{singleErr: err}
				}
				// continue to try the best to save required number of replicas
			}
			for j := range nextNodeGroupKeys {
				var workerPool util.WorkerPool
				pks := nextNodeGroupKeys[j]
				processedNodesMtx.RLock()
				nr := nodeResults[pks]
				processedNodesMtx.RUnlock()
				if nr.desc.local {
					workerPool = x.localPool
				} else {
					workerPool = x.remotePool
				}
				wg.Add(1)
				if err := workerPool.Submit(func() {
					defer wg.Done()
					err := f(nr.desc)
					processedNodesMtx.Lock()
					if nr.succeeded = err == nil; nr.succeeded {
						nodesCounters[listInd].stored++
					}
					nodeResults[pks] = nr
					processedNodesMtx.Unlock()
					if err != nil {
						lastRespErr.Store(err)
						svcutil.LogServiceError(l, "PUT", nr.desc.info.AddressGroup(), err)
						return
					}
				}); err != nil {
					wg.Done()
					err = fmt.Errorf("submit next job to save an object to the worker pool: %w", err)
					svcutil.LogWorkerPoolError(l, "PUT", err)
				}
			}
			wg.Wait()
		}
	}
	if !x.broadcast {
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
				nr.desc.info, nr.convertErr = x.convertNodeInfo(nodeLists[i][j])
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
			var workerPool util.WorkerPool
			if nr.desc.local {
				workerPool = x.localPool
			} else {
				workerPool = x.remotePool
			}
			wg.Add(1)
			if err := workerPool.Submit(func() {
				defer wg.Done()
				err := f(nr.desc)
				processedNodesMtx.Lock()
				// no need to update result details, just cache
				nodeResults[pks] = nr
				processedNodesMtx.Unlock()
				if err != nil {
					svcutil.LogServiceError(l, "PUT (extra broadcast)", nr.desc.info.AddressGroup(), err)
					return
				}
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

func (x placementIterator) convertNodeInfo(nodeInfo netmap.NodeInfo) (client.NodeInfo, error) {
	var res client.NodeInfo
	var endpoints network.AddressGroup
	if err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeInfo)); err != nil {
		return res, err
	}
	if ext := nodeInfo.ExternalAddresses(); len(ext) > 0 {
		var externalEndpoints network.AddressGroup
		if err := externalEndpoints.FromStringSlice(ext); err != nil {
			// less critical since the main ones must work, but also important
			x.log.Warn("failed to decode external network endpoints of the storage node from the network map, ignore them",
				zap.String("public key", netmap.StringifyPublicKey(nodeInfo)), zap.Strings("endpoints", ext), zap.Error(err))
		} else {
			res.SetExternalAddressGroup(externalEndpoints)
		}
	}
	res.SetAddressGroup(endpoints)
	res.SetPublicKey(nodeInfo.PublicKey())
	return res, nil
}
