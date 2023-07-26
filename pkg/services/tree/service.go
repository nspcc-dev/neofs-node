package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Service represents tree-service capable of working with multiple
// instances of CRDT trees.
type Service struct {
	cfg

	cache            clientCache
	replicateCh      chan movePair
	replicateLocalCh chan applyOp
	replicationTasks chan replicationTask
	closeCh          chan struct{}
	containerCache   containerCache

	syncChan chan struct{}
	syncPool *ants.Pool

	// cnrMap maps contrainer and tree ID to the minimum height which was fetched from _each_ client.
	// This allows us to better handle split-brain scenario, because we always synchronize
	// from the last seen height. The inner map is read-only and should not be modified in-place.
	cnrMap map[cidSDK.ID]map[string]uint64
	// cnrMapMtx protects cnrMap
	cnrMapMtx sync.Mutex
}

var _ TreeServiceServer = (*Service)(nil)

// New creates new tree service instance.
func New(opts ...Option) *Service {
	var s Service
	s.containerCacheSize = defaultContainerCacheSize
	s.replicatorChannelCapacity = defaultReplicatorCapacity
	s.replicatorWorkerCount = defaultReplicatorWorkerCount
	s.replicatorTimeout = defaultReplicatorSendTimeout

	for i := range opts {
		opts[i](&s.cfg)
	}

	if s.log == nil {
		s.log = zap.NewNop()
	}

	s.cache.init()
	s.closeCh = make(chan struct{})
	s.replicateCh = make(chan movePair, s.replicatorChannelCapacity)
	s.replicateLocalCh = make(chan applyOp)
	s.replicationTasks = make(chan replicationTask, s.replicatorWorkerCount)
	s.containerCache.init(s.containerCacheSize)
	s.cnrMap = make(map[cidSDK.ID]map[string]uint64)
	s.syncChan = make(chan struct{})
	s.syncPool, _ = ants.NewPool(defaultSyncWorkerCount)

	return &s
}

// Start starts the service.
func (s *Service) Start(ctx context.Context) {
	go s.replicateLoop(ctx)
	go s.syncLoop(ctx)

	select {
	case <-s.closeCh:
	case <-ctx.Done():
	default:
		// initial sync
		s.syncChan <- struct{}{}
	}
}

// Shutdown shutdowns the service.
func (s *Service) Shutdown() {
	close(s.closeCh)
	s.syncPool.Release()
}

func (s *Service) Add(ctx context.Context, req *AddRequest) (*AddResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectPut)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *AddResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.Add(ctx, req)
			return true
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	d := pilorama.CIDDescriptor{CID: cid, Position: pos, Size: len(ns)}
	log, err := s.forest.TreeMove(d, b.GetTreeId(), &pilorama.Move{
		Parent: b.GetParentId(),
		Child:  pilorama.RootID,
		Meta:   pilorama.Meta{Items: protoToMeta(b.GetMeta())},
	})
	if err != nil {
		return nil, err
	}

	s.pushToQueue(cid, b.GetTreeId(), log)
	return &AddResponse{
		Body: &AddResponse_Body{
			NodeId: log.Child,
		},
	}, nil
}

func (s *Service) AddByPath(ctx context.Context, req *AddByPathRequest) (*AddByPathResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectPut)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *AddByPathResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.AddByPath(ctx, req)
			return true
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	meta := protoToMeta(b.GetMeta())

	attr := b.GetPathAttribute()
	if len(attr) == 0 {
		attr = pilorama.AttributeFilename
	}

	d := pilorama.CIDDescriptor{CID: cid, Position: pos, Size: len(ns)}
	logs, err := s.forest.TreeAddByPath(d, b.GetTreeId(), attr, b.GetPath(), meta)
	if err != nil {
		return nil, err
	}

	for i := range logs {
		s.pushToQueue(cid, b.GetTreeId(), &logs[i])
	}

	nodes := make([]uint64, len(logs))
	nodes[0] = logs[len(logs)-1].Child
	for i, l := range logs[:len(logs)-1] {
		nodes[i+1] = l.Child
	}

	return &AddByPathResponse{
		Body: &AddByPathResponse_Body{
			Nodes: nodes,
		},
	}, nil
}

func (s *Service) Remove(ctx context.Context, req *RemoveRequest) (*RemoveResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectPut)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *RemoveResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.Remove(ctx, req)
			return true
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	if b.GetNodeId() == pilorama.RootID {
		return nil, fmt.Errorf("node with ID %d is root and can't be removed", b.GetNodeId())
	}

	d := pilorama.CIDDescriptor{CID: cid, Position: pos, Size: len(ns)}
	log, err := s.forest.TreeMove(d, b.GetTreeId(), &pilorama.Move{
		Parent: pilorama.TrashID,
		Child:  b.GetNodeId(),
	})
	if err != nil {
		return nil, err
	}

	s.pushToQueue(cid, b.GetTreeId(), log)
	return new(RemoveResponse), nil
}

// Move applies client operation to the specified tree and pushes in queue
// for replication on other nodes.
func (s *Service) Move(ctx context.Context, req *MoveRequest) (*MoveResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectPut)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *MoveResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.Move(ctx, req)
			return true
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	if b.GetNodeId() == pilorama.RootID {
		return nil, fmt.Errorf("node with ID %d is root and can't be moved", b.GetNodeId())
	}

	d := pilorama.CIDDescriptor{CID: cid, Position: pos, Size: len(ns)}
	log, err := s.forest.TreeMove(d, b.GetTreeId(), &pilorama.Move{
		Parent: b.GetParentId(),
		Child:  b.GetNodeId(),
		Meta:   pilorama.Meta{Items: protoToMeta(b.GetMeta())},
	})
	if err != nil {
		return nil, err
	}

	s.pushToQueue(cid, b.GetTreeId(), log)
	return new(MoveResponse), nil
}

func (s *Service) GetNodeByPath(ctx context.Context, req *GetNodeByPathRequest) (*GetNodeByPathResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectGet)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *GetNodeByPathResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.GetNodeByPath(ctx, req)
			return true
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	attr := b.GetPathAttribute()
	if len(attr) == 0 {
		attr = pilorama.AttributeFilename
	}

	nodes, err := s.forest.TreeGetByPath(cid, b.GetTreeId(), attr, b.GetPath(), b.GetLatestOnly())
	if err != nil {
		return nil, err
	}

	info := make([]*GetNodeByPathResponse_Info, 0, len(nodes))
	for _, node := range nodes {
		m, parent, err := s.forest.TreeGetMeta(cid, b.GetTreeId(), node)
		if err != nil {
			return nil, err
		}

		var x GetNodeByPathResponse_Info
		x.ParentId = parent
		x.NodeId = node
		x.Timestamp = m.Time
		if b.AllAttributes {
			x.Meta = metaToProto(m.Items)
		} else {
			for _, kv := range m.Items {
				for _, attr := range b.GetAttributes() {
					if kv.Key == attr {
						x.Meta = append(x.Meta, &KeyValue{
							Key:   kv.Key,
							Value: kv.Value,
						})
						break
					}
				}
			}
		}
		info = append(info, &x)
	}

	return &GetNodeByPathResponse{
		Body: &GetNodeByPathResponse_Body{
			Nodes: info,
		},
	}, nil
}

func (s *Service) GetSubTree(req *GetSubTreeRequest, srv TreeService_GetSubTreeServer) error {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return err
	}

	err := s.verifyClient(req, cid, b.GetBearerToken(), acl.OpObjectGet)
	if err != nil {
		return err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return err
	}
	if pos < 0 {
		var cli TreeService_GetSubTreeClient
		var outErr error
		err = s.forEachNode(srv.Context(), ns, func(c TreeServiceClient) bool {
			cli, outErr = c.GetSubTree(srv.Context(), req)
			return true
		})
		if err != nil {
			return err
		} else if outErr != nil {
			return outErr
		}
		for resp, err := cli.Recv(); err == nil; resp, err = cli.Recv() {
			if err := srv.Send(resp); err != nil {
				return err
			}
		}
		return nil
	}

	return getSubTree(srv, cid, b, s.forest)
}

func getSubTree(srv TreeService_GetSubTreeServer, cid cidSDK.ID, b *GetSubTreeRequest_Body, forest pilorama.Forest) error {
	// Traverse the tree in a DFS manner. Because we need to support arbitrary depth,
	// recursive implementation is not suitable here, so we maintain explicit stack.
	stack := [][]uint64{{b.GetRootId()}}

	for {
		if len(stack) == 0 {
			break
		} else if len(stack[len(stack)-1]) == 0 {
			stack = stack[:len(stack)-1]
			continue
		}

		nodeID := stack[len(stack)-1][0]
		stack[len(stack)-1] = stack[len(stack)-1][1:]

		m, p, err := forest.TreeGetMeta(cid, b.GetTreeId(), nodeID)
		if err != nil {
			return err
		}
		err = srv.Send(&GetSubTreeResponse{
			Body: &GetSubTreeResponse_Body{
				NodeId:    nodeID,
				ParentId:  p,
				Timestamp: m.Time,
				Meta:      metaToProto(m.Items),
			},
		})
		if err != nil {
			return err
		}

		if b.GetDepth() == 0 || uint32(len(stack)) < b.GetDepth() {
			children, err := forest.TreeGetChildren(cid, b.GetTreeId(), nodeID)
			if err != nil {
				return err
			}
			if len(children) != 0 {
				stack = append(stack, children)
			}
		}
	}
	return nil
}

// Apply locally applies operation from the remote node to the tree.
func (s *Service) Apply(_ context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	err := verifyMessage(req)
	if err != nil {
		return nil, err
	}

	var cid cidSDK.ID
	if err := cid.Decode(req.GetBody().GetContainerId()); err != nil {
		return nil, err
	}

	key := req.GetSignature().GetKey()

	_, pos, size, err := s.getContainerInfo(cid, key)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		return nil, errors.New("`Apply` request must be signed by a container node")
	}

	op := req.GetBody().GetOperation()

	var meta pilorama.Meta
	if err := meta.FromBytes(op.GetMeta()); err != nil {
		return nil, fmt.Errorf("can't parse meta-information: %w", err)
	}

	select {
	case s.replicateLocalCh <- applyOp{
		treeID:        req.GetBody().GetTreeId(),
		CIDDescriptor: pilorama.CIDDescriptor{CID: cid, Position: pos, Size: size},
		Move: pilorama.Move{
			Parent: op.GetParentId(),
			Child:  op.GetChildId(),
			Meta:   meta,
		},
	}:
	default:
	}
	return &ApplyResponse{Body: &ApplyResponse_Body{}, Signature: &Signature{}}, nil
}

func (s *Service) GetOpLog(req *GetOpLogRequest, srv TreeService_GetOpLogServer) error {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(req.GetBody().GetContainerId()); err != nil {
		return err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return err
	}
	if pos < 0 {
		var cli TreeService_GetOpLogClient
		var outErr error
		err := s.forEachNode(srv.Context(), ns, func(c TreeServiceClient) bool {
			cli, outErr = c.GetOpLog(srv.Context(), req)
			return true
		})
		if err != nil {
			return err
		} else if outErr != nil {
			return outErr
		}
		for resp, err := cli.Recv(); err == nil; resp, err = cli.Recv() {
			if err := srv.Send(resp); err != nil {
				return err
			}
		}
		return nil
	}

	h := b.GetHeight()
	for {
		lm, err := s.forest.TreeGetOpLog(cid, b.GetTreeId(), h)
		if err != nil || lm.Time == 0 {
			return err
		}

		err = srv.Send(&GetOpLogResponse{
			Body: &GetOpLogResponse_Body{
				Operation: &LogMove{
					ParentId: lm.Parent,
					Meta:     lm.Meta.Bytes(),
					ChildId:  lm.Child,
				},
			},
		})
		if err != nil {
			return err
		}

		h = lm.Time + 1
	}
}

func (s *Service) TreeList(ctx context.Context, req *TreeListRequest) (*TreeListResponse, error) {
	var cid cidSDK.ID

	err := cid.Decode(req.GetBody().GetContainerId())
	if err != nil {
		return nil, err
	}

	// just verify the signature, not ACL checks
	// since tree ID list is not protected like
	// the containers list
	err = verifyMessage(req)
	if err != nil {
		return nil, err
	}

	ns, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		var resp *TreeListResponse
		var outErr error
		err = s.forEachNode(ctx, ns, func(c TreeServiceClient) bool {
			resp, outErr = c.TreeList(ctx, req)
			return outErr == nil
		})
		if err != nil {
			return nil, err
		}
		return resp, outErr
	}

	ids, err := s.forest.TreeList(cid)
	if err != nil {
		return nil, err
	}

	return &TreeListResponse{
		Body: &TreeListResponse_Body{
			Ids: ids,
		},
	}, nil
}

func protoToMeta(arr []*KeyValue) []pilorama.KeyValue {
	meta := make([]pilorama.KeyValue, len(arr))
	for i, kv := range arr {
		if kv != nil {
			meta[i].Key = kv.Key
			meta[i].Value = kv.Value
		}
	}
	return meta
}

func metaToProto(arr []pilorama.KeyValue) []*KeyValue {
	meta := make([]*KeyValue, len(arr))
	for i, kv := range arr {
		meta[i] = &KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}
	return meta
}

// getContainerInfo returns the list of container nodes, position in the container for the node
// with pub key and total amount of nodes in all replicas.
func (s *Service) getContainerInfo(cid cidSDK.ID, pub []byte) ([]netmapSDK.NodeInfo, int, int, error) {
	cntNodes, _, err := s.getContainerNodes(cid)
	if err != nil {
		return nil, 0, 0, err
	}

	for i, node := range cntNodes {
		if bytes.Equal(node.PublicKey(), pub) {
			return cntNodes, i, len(cntNodes), nil
		}
	}
	return cntNodes, -1, len(cntNodes), nil
}

func (s *Service) Healthcheck(context.Context, *HealthcheckRequest) (*HealthcheckResponse, error) {
	return new(HealthcheckResponse), nil
}
