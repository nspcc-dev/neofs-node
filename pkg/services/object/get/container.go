package getsvc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (exec *execCtx) executeOnContainer() {
	if exec.isLocal() {
		exec.log.Debug("return result directly")
		return
	}

	exec.log.Debug("trying to execute in container...")

	addr := exec.address()

	nodeLists := exec.nodeLists
	primaryCounts := exec.repRules
	var ecRules []iec.Rule
	if nodeLists == nil {
		var err error
		nodeLists, primaryCounts, ecRules, err = exec.svc.neoFSNet.GetNodesForObject(addr)
		if err != nil {
			exec.status = statusUndefined
			exec.err = err
			exec.log.Debug("failed to list storage nodes for the object", zap.Error(err))
			return
		}
	}

	ctx, cancel := context.WithCancel(exec.context())
	defer cancel()

	exec.status = statusUndefined
	mProcessedNodes := make(map[string]struct{})
	var endpoints network.AddressGroup
	var j, jLim uint
	primary := true

	for i := 0; i < len(primaryCounts); i++ { // do not use for-range!
		if primary {
			j, jLim = 0, primaryCounts[i]
		} else {
			j, jLim = primaryCounts[i], uint(len(nodeLists[i]))
		}

		for ; j < jLim; j++ {
			select {
			case <-ctx.Done():
				exec.log.Debug("interrupt placement iteration by context",
					zap.Error(ctx.Err()),
				)

				return
			default:
			}

			bKey := nodeLists[i][j].PublicKey()
			strKey := string(bKey)
			if _, ok := mProcessedNodes[strKey]; ok || exec.svc.neoFSNet.IsLocalNodePublicKey(bKey) {
				continue
			}

			mProcessedNodes[strKey] = struct{}{}

			if err := endpoints.FromIterator(network.NodeEndpointsIterator(nodeLists[i][j])); err != nil {
				// critical error that may ultimately block the storage service. Normally it
				// should not appear because entry into the network map under strict control
				exec.log.Error("failed to decode network endpoints of the storage node from the network map, skip the node",
					zap.String("public key", netmap.StringifyPublicKey(nodeLists[i][j])), zap.Error(err))
				continue
			}

			// TODO: #1142 consider parallel execution
			// TODO: #1142 consider optimization: if status == SPLIT we can continue until
			//  we reach the best result - split info with linking object ID.
			var info client.NodeInfo
			info.SetAddressGroup(endpoints)
			info.SetPublicKey(bKey)

			if exec.processNode(info) {
				exec.log.Debug("completing the operation")
				return
			}
		}

		if primary && i == len(primaryCounts)-1 {
			// switch to reserve nodes
			primary = false
			i = -1
		}
	}

	if len(ecRules) == 0 {
		return
	}

	localNodeKey, err := exec.svc.keyStore.GetKey(nil)
	if err != nil {
		exec.err = fmt.Errorf("get local SN private key: %w", err)
		return
	}

	cnr := addr.Container()
	id := addr.Object()
	sTok := exec.prm.common.SessionToken()
	bTok := exec.prm.common.BearerToken()

	var hdrAtomic atomic.Value // object.Object

	eg, egCtx := errgroup.WithContext(ctx)
	for i := range ecRules {
		totalParts := int(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		eg.SetLimit(min(totalParts, 3))

		nodeList := nodeLists[len(primaryCounts)+i]
		for j := range nodeList {
			node := nodeList[j]
			eg.Go(func() error {
				// errgroup call the function even when context is done. Routines for backup
				// nodes should catch this asap to do nothing.
				if err := egCtx.Err(); err != nil {
					return err
				}

				hdr, err := exec.svc.conns.Head(egCtx, node, *localNodeKey, cnr, id, sTok, bTok)
				if err == nil {
					hdrAtomic.CompareAndSwap(nil, hdr)
					return errInterrupt
				}

				if errors.Is(err, apistatus.ErrObjectAlreadyRemoved) || errors.Is(err, egCtx.Err()) {
					return err
				}

				if !errors.Is(err, apistatus.ErrObjectNotFound) {
					exec.log.Debug("failed to HEAD object from remote node",
						zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Error(err))
				}

				return nil
			})
		}

		err := eg.Wait()
		if err != nil {
			if !errors.Is(err, errInterrupt) {
				exec.err = err
				return
			}
			break
		}
	}

	hdrVal := hdrAtomic.Load()
	if hdrVal == nil {
		return
	}

	hdr := hdrVal.(object.Object)
	exec.collectedHeader = &hdr
	exec.writeCollectedHeader()
}
