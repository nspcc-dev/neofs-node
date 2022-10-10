package tree

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrNotInContainer is returned when operation could not be performed
// because the node is not included in the container.
var ErrNotInContainer = errors.New("node is not in container")

// Synchronize tries to synchronize log starting from the last stored height.
func (s *Service) Synchronize(ctx context.Context, cid cid.ID, treeID string) error {
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

	lm, err := s.forest.TreeGetOpLog(cid, treeID, 0)
	if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
		return err
	}

	height := lm.Time + 1
	for _, n := range nodes {
		n.IterateNetworkEndpoints(func(addr string) bool {
			var a network.Address
			if err := a.FromString(addr); err != nil {
				return false
			}

			cc, err := grpc.DialContext(ctx, a.URIAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Failed to connect, try the next address.
				return false
			}

			treeClient := NewTreeServiceClient(cc)
			for {
				h, err := s.synchronizeSingle(ctx, d, treeID, height, treeClient)
				if height < h {
					height = h
				}
				if err != nil || h <= height {
					// Error with the response, try the next node.
					return true
				}
			}
		})
	}
	return nil
}

func (s *Service) synchronizeSingle(ctx context.Context, d pilorama.CIDDescriptor, treeID string, height uint64, treeClient TreeServiceClient) (uint64, error) {
	rawCID := make([]byte, sha256.Size)
	d.CID.Encode(rawCID)

	for {
		newHeight := height
		req := &GetOpLogRequest{
			Body: &GetOpLogRequest_Body{
				ContainerId: rawCID,
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
			if err := s.forest.TreeApply(d, treeID, m); err != nil {
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
