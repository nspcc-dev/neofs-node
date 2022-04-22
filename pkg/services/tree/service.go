package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

// Service represents tree-service capable of working with multiple
// instances of CRDT trees.
type Service struct {
	cfg

	replicateCh chan movePair
	closeCh     chan struct{}
}

var _ TreeServiceServer = (*Service)(nil)

// New creates new tree service instance.
func New(opts ...Option) *Service {
	var s Service
	for i := range opts {
		opts[i](&s.cfg)
	}

	if s.log == nil {
		s.log = zap.NewNop()
	}

	s.closeCh = make(chan struct{})
	s.replicateCh = make(chan movePair, defaultReplicatorCapacity)

	return &s
}

// Start starts the service.
func (s *Service) Start(ctx context.Context) {
	go s.replicateLoop(ctx)
}

// Shutdown shutdowns the service.
func (s *Service) Shutdown() {
	close(s.closeCh)
}

func (s *Service) Add(_ context.Context, req *AddRequest) (*AddResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, req.GetSignature().GetKey())
	if err != nil {
		return nil, err
	}

	log, err := s.forest.TreeMove(cid, b.GetTreeId(), &pilorama.Move{
		Parent: b.GetParentId(),
		Child:  pilorama.RootID,
		Meta:   pilorama.Meta{Items: constructMeta(b.GetMeta())},
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

func (s *Service) AddByPath(_ context.Context, req *AddByPathRequest) (*AddByPathResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, req.GetSignature().GetKey())
	if err != nil {
		return nil, err
	}

	meta := constructMeta(b.GetMeta())

	attr := b.GetPathAttribute()
	if len(attr) == 0 {
		attr = pilorama.AttributeFilename
	}

	logs, err := s.forest.TreeAddByPath(cid, b.GetTreeId(), attr, b.GetPath(), meta)
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

func (s *Service) Remove(_ context.Context, req *RemoveRequest) (*RemoveResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, req.GetSignature().GetKey())
	if err != nil {
		return nil, err
	}

	if b.GetNodeId() == pilorama.RootID {
		return nil, fmt.Errorf("node with ID %d is root and can't be removed", b.GetNodeId())
	}

	log, err := s.forest.TreeMove(cid, b.GetTreeId(), &pilorama.Move{
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
func (s *Service) Move(_ context.Context, req *MoveRequest) (*MoveResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
	}

	err := s.verifyClient(req, cid, req.GetSignature().GetKey())
	if err != nil {
		return nil, err
	}

	if b.GetNodeId() == pilorama.RootID {
		return nil, fmt.Errorf("node with ID %d is root and can't be moved", b.GetNodeId())
	}

	log, err := s.forest.TreeMove(cid, b.GetTreeId(), &pilorama.Move{
		Parent: b.GetParentId(),
		Child:  b.GetNodeId(),
		Meta:   pilorama.Meta{Items: constructMeta(b.GetMeta())},
	})
	if err != nil {
		return nil, err
	}

	s.pushToQueue(cid, b.GetTreeId(), log)
	return new(MoveResponse), nil
}

func (s *Service) GetNodeByPath(_ context.Context, req *GetNodeByPathRequest) (*GetNodeByPathResponse, error) {
	b := req.GetBody()

	var cid cidSDK.ID
	if err := cid.Decode(b.GetContainerId()); err != nil {
		return nil, err
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
		m, err := s.forest.TreeGetMeta(cid, b.GetTreeId(), node)
		if err != nil {
			return nil, err
		}

		var x GetNodeByPathResponse_Info
		x.NodeId = node
		x.Timestamp = m.Time
		for _, kv := range m.Items {
			needAttr := b.AllAttributes
			if !needAttr {
				for _, attr := range b.GetAttributes() {
					if kv.Key == attr {
						needAttr = true
						break
					}
				}
			}

			if needAttr {
				x.Meta = append(x.Meta, &KeyValue{
					Key:   kv.Key,
					Value: kv.Value,
				})
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

func (s *Service) GetSubTree(_ context.Context, req *GetSubTreeRequest) (*GetSubTreeResponse, error) {
	return nil, errors.New("GetSubTree is unimplemented")
}

// Apply locally applies operation from the remote node to the tree.
func (s *Service) Apply(_ context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	err := signature.VerifyServiceMessage(req)
	if err != nil {
		return nil, err
	}

	var cid cidSDK.ID
	if err := cid.Decode(req.GetBody().GetContainerId()); err != nil {
		return nil, err
	}

	found := false
	key := req.GetSignature().GetKey()
	nodes, _ := s.getContainerNodes(cid)

loop:
	for _, n := range nodes {
		if bytes.Equal(key, n.PublicKey()) {
			found = true
			break loop
		}
	}
	if !found {
		return nil, errors.New("`Apply` request must be signed by a container node")
	}

	op := req.GetBody().GetOperation()

	var meta pilorama.Meta
	if err := meta.FromBytes(op.GetMeta()); err != nil {
		return nil, fmt.Errorf("can't parse meta-information: %w", err)
	}

	return nil, s.forest.TreeApply(cid, req.GetBody().GetTreeId(), &pilorama.Move{
		Parent: op.GetParentId(),
		Child:  op.GetChildId(),
		Meta:   meta,
	})
}

func constructMeta(arr []*KeyValue) []pilorama.KeyValue {
	meta := make([]pilorama.KeyValue, len(arr))
	for i, kv := range arr {
		meta[i].Key = kv.Key
		meta[i].Value = kv.Value
	}
	return meta
}
