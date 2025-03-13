package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrNotInContainer is returned when operation could not be performed
// because the node is not included in the container.
var ErrNotInContainer = errors.New("node is not in container")

const defaultSyncWorkerCount = 20

// synchronizeAllTrees synchronizes all the trees of the container. It fetches
// tree IDs from the other container nodes. Returns ErrNotInContainer if the node
// is not included in the container.
func (s *Service) synchronizeAllTrees(ctx context.Context, cid cid.ID) error {
	nodes, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes: %w", err)
	}

	if pos < 0 {
		return ErrNotInContainer
	}

	var d pilorama.CIDDescriptor
	d.CID = cid
	d.Position = pos
	d.Size = len(nodes)

	nodes = randomizeNodeOrder(nodes, pos)
	if len(nodes) == 0 {
		return nil
	}

	req := &TreeListRequest{
		Body: &TreeListRequest_Body{
			ContainerId: cid[:],
		},
	}

	err = SignMessage(req, s.key)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *TreeListResponse
	var treesToSync []string
	var outErr error

	err = s.forEachNode(ctx, nodes, func(c TreeServiceClient) bool {
		resp, outErr = c.TreeList(ctx, req)
		if outErr != nil {
			return false
		}

		treesToSync = resp.GetBody().GetIds()

		return true
	})
	if err != nil {
		outErr = err
	}

	if outErr != nil {
		return fmt.Errorf("could not fetch tree ID list: %w", outErr)
	}

	s.cnrMapMtx.Lock()
	oldStatus := s.cnrMap[cid]
	s.cnrMapMtx.Unlock()

	syncStatus := map[string]uint64{}
	for i := range treesToSync {
		syncStatus[treesToSync[i]] = 0
	}
	for tid := range oldStatus {
		if _, ok := syncStatus[tid]; ok {
			syncStatus[tid] = oldStatus[tid]
		}
	}

	for _, tid := range treesToSync {
		h := s.synchronizeTree(ctx, d, syncStatus[tid], tid, nodes)
		if syncStatus[tid] < h {
			syncStatus[tid] = h
		}
	}

	s.cnrMapMtx.Lock()
	s.cnrMap[cid] = syncStatus
	s.cnrMapMtx.Unlock()

	return nil
}

// SynchronizeTree tries to synchronize log starting from the last stored height.
func (s *Service) SynchronizeTree(ctx context.Context, cid cid.ID, treeID string) error {
	nodes, pos, err := s.getContainerNodes(cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes: %w", err)
	}

	if pos < 0 {
		return ErrNotInContainer
	}

	var d pilorama.CIDDescriptor
	d.CID = cid
	d.Position = pos
	d.Size = len(nodes)

	nodes = randomizeNodeOrder(nodes, pos)
	if len(nodes) == 0 {
		return nil
	}

	s.synchronizeTree(ctx, d, 0, treeID, nodes)
	return nil
}

func (s *Service) synchronizeTree(ctx context.Context, d pilorama.CIDDescriptor, from uint64,
	treeID string, nodes []netmapSDK.NodeInfo) uint64 {
	s.log.Debug("synchronize tree",
		zap.Stringer("cid", d.CID),
		zap.String("tree", treeID),
		zap.Uint64("from", from))

	newHeight := uint64(math.MaxUint64)
	for _, n := range nodes {
		height := from
	loop:
		for addr := range n.NetworkEndpoints() {
			var a network.Address
			if err := a.FromString(addr); err != nil {
				continue
			}

			cc, err := grpc.NewClient(a.URIAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Failed to connect, try the next address.
				continue
			}

			treeClient := NewTreeServiceClient(cc)
			for {
				h, err := s.synchronizeSingle(ctx, d, treeID, height, treeClient)
				if height < h {
					height = h
				}
				if err != nil || h <= height {
					_ = cc.Close()
					// Error with the response, try the next node.
					break loop
				}
			}
		}
		if height <= from { // do not increase starting height on fail
			newHeight = from
		} else if height < newHeight { // take minimum across all clients
			newHeight = height
		}
	}
	if newHeight == math.MaxUint64 {
		newHeight = from
	}
	return newHeight
}

func (s *Service) synchronizeSingle(ctx context.Context, d pilorama.CIDDescriptor, treeID string, height uint64, treeClient TreeServiceClient) (uint64, error) {
	for {
		newHeight := height
		req := &GetOpLogRequest{
			Body: &GetOpLogRequest_Body{
				ContainerId: d.CID[:],
				TreeId:      treeID,
				Height:      newHeight,
			},
		}
		if err := SignMessage(req, s.key); err != nil {
			return newHeight, err
		}

		c, err := treeClient.GetOpLog(ctx, req)
		if err != nil {
			return newHeight, fmt.Errorf("can't initialize client: %w", err)
		}

		res, err := c.Recv()
		for ; err == nil; res, err = c.Recv() {
			lm := res.GetBody().GetOperation()
			m := &pilorama.Move{
				Parent: lm.ParentId,
				Child:  lm.ChildId,
			}
			if err := m.Meta.FromBytes(lm.Meta); err != nil {
				return newHeight, err
			}
			if err := s.forest.TreeApply(d, treeID, m, true); err != nil {
				return newHeight, err
			}
			if m.Time > newHeight {
				newHeight = m.Time + 1
			} else {
				newHeight++
			}
		}
		if height == newHeight || err != nil && !errors.Is(err, io.EOF) {
			return newHeight, err
		}
		height = newHeight
	}
}

// ErrAlreadySyncing is returned when a service synchronization has already
// been started.
var ErrAlreadySyncing = errors.New("service is being synchronized")

// ErrShuttingDown is returned when the service is shitting down and could not
// accept any calls.
var ErrShuttingDown = errors.New("service is shutting down")

// SynchronizeAll forces tree service to synchronize all the trees according to
// netmap information. Must not be called before Service.Start.
// Returns ErrAlreadySyncing if synchronization has been started and blocked
// by another routine.
// Note: non-blocking operation.
func (s *Service) SynchronizeAll() error {
	select {
	case <-s.closeCh:
		return ErrShuttingDown
	default:
	}

	select {
	case s.syncChan <- struct{}{}:
		return nil
	default:
		return ErrAlreadySyncing
	}
}

func (s *Service) syncLoop(ctx context.Context) {
	for {
		select {
		case <-s.closeCh:
			return
		case <-ctx.Done():
			return
		case <-s.syncChan:
			s.log.Debug("syncing trees...")

			cnrs, err := s.cfg.cnrSource.List()
			if err != nil {
				s.log.Error("could not fetch containers", zap.Error(err))
				continue
			}

			newMap := make(map[cid.ID]struct{}, len(s.cnrMap))
			cnrsToSync := make([]cid.ID, 0, len(cnrs))

			var removed []cid.ID
			for _, cnr := range cnrs {
				_, pos, err := s.getContainerNodes(cnr)
				if err != nil {
					s.log.Error("could not calculate container nodes",
						zap.Stringer("cid", cnr),
						zap.Error(err))
					removed = append(removed, cnr)
					continue
				}

				if pos < 0 {
					// node is not included in the container.
					continue
				}

				newMap[cnr] = struct{}{}
				cnrsToSync = append(cnrsToSync, cnr)
			}

			// sync new containers
			var wg sync.WaitGroup
			for _, cnr := range cnrsToSync {
				wg.Add(1)
				err := s.syncPool.Submit(func() {
					defer wg.Done()
					s.log.Debug("syncing container trees...", zap.Stringer("cid", cnr))

					err := s.synchronizeAllTrees(ctx, cnr)
					if err != nil {
						s.log.Error("could not sync trees", zap.Stringer("cid", cnr), zap.Error(err))
						return
					}

					s.log.Debug("container trees have been synced", zap.Stringer("cid", cnr))
				})
				if err != nil {
					wg.Done()
					s.log.Error("could not query trees for synchronization",
						zap.Stringer("cid", cnr),
						zap.Error(err))
					if errors.Is(err, ants.ErrPoolClosed) {
						return
					}
				}
			}
			wg.Wait()

			s.cnrMapMtx.Lock()
			for cnr := range s.cnrMap {
				if _, ok := newMap[cnr]; ok {
					continue
				}
				removed = append(removed, cnr)
			}
			for i := range removed {
				delete(s.cnrMap, removed[i])
			}
			s.cnrMapMtx.Unlock()

			for _, cnr := range removed {
				s.log.Debug("removing redundant trees...", zap.Stringer("cid", cnr))

				err = s.DropTree(ctx, cnr, "")
				if err != nil {
					s.log.Error("could not remove redundant tree",
						zap.Stringer("cid", cnr),
						zap.Error(err))
					continue
				}
			}

			s.log.Debug("trees have been synchronized")
		}
	}
}

// randomizeNodeOrder shuffles nodes and removes not a `pos` index.
// It is assumed that 0 <= pos < len(nodes).
func randomizeNodeOrder(cnrNodes []netmap.NodeInfo, pos int) []netmap.NodeInfo {
	if len(cnrNodes) == 1 {
		return nil
	}

	nodes := slices.Concat(cnrNodes[:pos], cnrNodes[pos+1:])
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes
}
