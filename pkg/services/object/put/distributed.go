package putsvc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type preparedObjectTarget interface {
	WriteObject(*objectSDK.Object, object.ContentMeta) error
	Close() (oid.ID, error)
}

type distributedTarget struct {
	// whether to perform additional best-effort of sending the object replica to
	// all reserve nodes of the container
	broadcast bool

	remotePool, localPool util.WorkerPool

	obj     *objectSDK.Object
	objMeta object.ContentMeta

	payload []byte

	nodeTargetInitializer func(nodeDesc) preparedObjectTarget

	node Node

	relay func(nodeDesc) error

	fmt *object.FormatValidator

	log *zap.Logger

	objStoragePolicy ObjectStoragePolicy

	// when non-zero, this setting simplifies the object's storage policy
	// requirements to a fixed number of object replicas to be retained
	linearReplNum uint32

	// when set, the object is saved on the local node only
	localOnly bool
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

func (t *distributedTarget) WriteHeader(obj *objectSDK.Object) error {
	t.obj = obj

	return nil
}

func (t *distributedTarget) Write(p []byte) (n int, err error) {
	t.payload = append(t.payload, p...)

	return len(p), nil
}

func (t *distributedTarget) Close() (oid.ID, error) {
	defer func() {
		putPayload(t.payload)
		t.payload = nil
	}()

	t.obj.SetPayload(t.payload)

	var err error

	if t.objMeta, err = t.fmt.ValidateContent(t.obj); err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
	}

	if len(t.obj.Children()) > 0 {
		// enabling extra broadcast for linking objects
		t.broadcast = true
	}

	return t.iteratePlacement(t.sendObject)
}

func (t *distributedTarget) sendObject(node nodeDesc) error {
	if !node.local && t.relay != nil {
		return t.relay(node)
	}

	target := t.nodeTargetInitializer(node)

	if err := target.WriteObject(t.obj, t.objMeta); err != nil {
		return fmt.Errorf("could not write header: %w", err)
	} else if _, err := target.Close(); err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}
	return nil
}

func (t *distributedTarget) convertNodeInfo(nodeInfo netmap.NodeInfo) (client.NodeInfo, error) {
	var res client.NodeInfo

	var endpoints network.AddressGroup
	err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeInfo))
	if err != nil {
		return res, err
	}

	if ext := nodeInfo.ExternalAddresses(); len(ext) > 0 {
		var externalEndpoints network.AddressGroup
		err = externalEndpoints.FromStringSlice(ext)
		if err != nil {
			// less critical since the main ones must work, but also important
			t.log.Warn("failed to decode external network endpoints of the storage node from the network map, ignore them",
				zap.String("public key", netmap.StringifyPublicKey(nodeInfo)),
				zap.Strings("endpoints", ext), zap.Error(err))
		} else {
			res.SetExternalAddressGroup(externalEndpoints)
		}
	}

	res.SetAddressGroup(endpoints)
	res.SetPublicKey(nodeInfo.PublicKey())

	return res, nil
}

func (t *distributedTarget) iteratePlacement(f func(nodeDesc) error) (oid.ID, error) {
	id, _ := t.obj.ID()

	nodeLists, primaryNodeNums, err := t.objStoragePolicy.StorageNodesForObject(id)
	if err != nil {
		return oid.ID{}, fmt.Errorf("sort container nodes for the object: %w", err)
	}

	if t.localOnly {
		localNodeInLists := false
	loop:
		for i := range nodeLists {
			for j := range nodeLists[i] {
				if localNodeInLists = t.node.IsLocalPublicKey(nodeLists[i][j].PublicKey()); localNodeInLists {
					if t.linearReplNum > 1 {
						return oid.ID{}, errors.New("more than one replica is requested in a local operation")
					}

					nodeLists = [][]netmap.NodeInfo{{nodeLists[i][j]}}
					primaryNodeNums = []uint32{1}

					break loop
				}
			}
		}

		if !localNodeInLists {
			return oid.ID{}, errors.New("local node does not comply with container storage policy")
		}
	} else if t.linearReplNum > 0 {
		var sumLen int
		for i := range nodeLists {
			sumLen += len(nodeLists[i])
		}

		flatList := make([]netmap.NodeInfo, 0, sumLen)

		for i := range nodeLists {
			flatList = append(flatList, nodeLists[i]...)
		}

		nodeLists = [][]netmap.NodeInfo{flatList}
		primaryNodeNums = []uint32{t.linearReplNum}
		// FIXME: current approach works from the client POV, but may be quite
		//  suboptimal from a system POV. For example, consider following node lists:
		//   1 replica in [N1, N2]
		//   1 replica in [N3, N4]
		//  and t.linearReplNum = 2. If everything is OK with nodes, in current
		//  implementation the object will be stored on {}N1, N2} nodes. At the same
		//  time, N2 is a reserve node while N3 is a primary one in terms of the storage
		//  policy. This behavior provokes the system in advance to background recording
		//  of an object on node N3 and subsequent deletion from the N2 one
		//  (policer+replicator components).
	}

	// container nodes on which a replica of the current object has already been saved.
	// Values indicate whether the operation was successful.
	processedNodes := make(map[string]bool)
	var processedNodesMtx sync.Mutex

	var resErr atomic.Value

nextNodeList:
	for i := range nodeLists {
		var savedReplicas uint32
		// legit cast since list length is >= then primary num
		needReplNum := int(primaryNodeNums[i])
		// nodes to which the object record is parallelized per iteration
		curNodeGroup := make([]nodeDesc, 0, needReplNum)
		// TODO: processing lists in ascending size can potentially reduce failure latency
		//  and volume of "unfinished" data

		for nAlreadyProcessed := 0; ; {
			curNodeGroup = curNodeGroup[:0]

			for j := nAlreadyProcessed; j < len(nodeLists[i]) && int(savedReplicas)+len(curNodeGroup) < needReplNum; nAlreadyProcessed, j = nAlreadyProcessed+1, j+1 {
				// condition here makes no sense for the 1st iteration, but it makes code simpler:
				// otherwise, we should check this before each 'continue' below
				if int(savedReplicas)+len(curNodeGroup)+len(nodeLists[i])-j < needReplNum {
					lastRespNodeErr, _ := resErr.Load().(error)
					return oid.ID{}, errIncompletePut{singleErr: lastRespNodeErr}
				}

				bNodePubKey := nodeLists[i][j].PublicKey()
				strNodePubKey := string(bNodePubKey)
				if succeeded, ok := processedNodes[strNodePubKey]; ok {
					if succeeded {
						savedReplicas++
						if int(savedReplicas) == needReplNum {
							continue nextNodeList
						}
					}
					continue
				}

				var node nodeDesc
				node.local = t.node.IsLocalPublicKey(bNodePubKey)

				if !node.local {
					node.info, err = t.convertNodeInfo(nodeLists[i][j])
					if err != nil {
						// critical error that may ultimately block the storage service. Normally it
						// should not appear because entry into the network map under strict control.
						t.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
							zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(err))
						processedNodes[strNodePubKey] = false
						continue
					}
				}

				curNodeGroup = append(curNodeGroup, node)
			}

			// we check this in loop above, but not for the last element of the list
			if int(savedReplicas)+len(curNodeGroup) < needReplNum {
				lastRespNodeErr, _ := resErr.Load().(error)
				return oid.ID{}, errIncompletePut{singleErr: lastRespNodeErr}
			}

			var wg sync.WaitGroup

			// send object to the prepared group of storage nodes in parallel
			for j := range curNodeGroup {
				var workerPool util.WorkerPool
				if curNodeGroup[j].local {
					workerPool = t.localPool
				} else {
					workerPool = t.remotePool
				}

				jCp := j
				wg.Add(1)

				if err := workerPool.Submit(func() {
					defer wg.Done()

					err := f(curNodeGroup[jCp])

					processedNodesMtx.Lock()
					processedNodes[string(curNodeGroup[jCp].info.PublicKey())] = err == nil
					processedNodesMtx.Unlock()

					if err != nil {
						resErr.Store(err)
						svcutil.LogServiceError(t.log, "PUT", curNodeGroup[jCp].info.AddressGroup(), err)
						return
					}

					atomic.AddUint32(&savedReplicas, 1)
				}); err != nil {
					svcutil.LogWorkerPoolError(t.log, "PUT", err)
					// FIXME: service must be denied if the local pool is overloaded while the
					//  remote one is free
					// TODO: check all running jobs are interrupted in this case (they must be)
					// TODO: this case is worth to be distinguished from general 'incomplete' one.
					//  HTTP '503 Service Unavailable'-like status would be nice
					lastRespNodeErr, _ := resErr.Load().(error)
					return oid.ID{}, errIncompletePut{singleErr: lastRespNodeErr}
				}
			}

			wg.Wait()

			if int(savedReplicas) == needReplNum {
				break
			}
		}
	}

	// FIXME: the main operation has already been completed, server should
	//  immediately notify the client about this

	if t.broadcast {
		var wg sync.WaitGroup

	broadcast:
		for i := range nodeLists {
			for j := range nodeLists[i] {
				bNodePubKey := nodeLists[i][j].PublicKey()
				strNodePubKey := string(bNodePubKey)
				if _, ok := processedNodes[strNodePubKey]; ok {
					continue
				}

				var node nodeDesc
				node.local = t.node.IsLocalPublicKey(bNodePubKey)

				if !node.local {
					node.info, err = t.convertNodeInfo(nodeLists[i][j])
					if err != nil {
						// critical error that may ultimately block the storage service. Normally it
						// should not appear because entry into the network map under strict control.
						t.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
							zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(err))
						processedNodes[strNodePubKey] = false
						continue
					}
				}

				var workerPool util.WorkerPool
				if node.local {
					workerPool = t.localPool
				} else {
					workerPool = t.remotePool
				}

				wg.Add(1)
				if err := workerPool.Submit(func() {
					defer wg.Done()

					err := f(node)

					processedNodesMtx.Lock()
					processedNodes[strNodePubKey] = err == nil
					processedNodesMtx.Unlock()

					if err != nil {
						svcutil.LogServiceError(t.log, "PUT BROADCAST", node.info.AddressGroup(), err)
						return
					}
				}); err != nil {
					wg.Done()
					svcutil.LogWorkerPoolError(t.log, "PUT BROADCAST", err)
					break broadcast
				}
			}
		}

		wg.Wait()
	}

	return id, nil
}
