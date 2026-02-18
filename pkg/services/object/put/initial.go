package putsvc

import (
	"fmt"
	"iter"
	"slices"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type InitialPlacementPolicy struct {
	MaxReplicas        uint32
	PreferLocal        bool
	MaxPerRuleReplicas []uint32
}

type progressStatus uint8

const (
	statusUndone progressStatus = iota
	statusWIP
	statusOK
	statusFail
)

type nodeProgress struct {
	status progressStatus
	ok     bool
}

func (t *distributedTarget) handleInitialPlacementPolicy(inPolicy InitialPlacementPolicy, mainRepRules []uint, mainECRules []iec.Rule,
	nodeLists [][]netmap.NodeInfo, obj object.Object, encObj encodedObject) error {
	var repLimits, ecLimits []uint32
	if inPolicy.MaxPerRuleReplicas != nil {
		repLimits = inPolicy.MaxPerRuleReplicas[:len(mainRepRules)]
		ecLimits = inPolicy.MaxPerRuleReplicas[len(mainRepRules):]
	} else {
		// TODO: make ContainerNodes.PrimaryCounts() to just assign here
		repLimits = make([]uint32, len(mainRepRules))
		for i := range repLimits {
			repLimits[i] = uint32(mainRepRules[i])
		}
		ecLimits = islices.RepeatElement(len(mainECRules), uint32(1))
	}

	listStatuses := make([][]nodeProgress, len(repLimits)+len(ecLimits))
	for i := range listStatuses {
		if i < len(repLimits) {
			if repLimits[i] > 0 {
				listStatuses[i] = make([]nodeProgress, len(nodeLists[i]))
			}
			continue
		}

		if ecLimits[i-len(repLimits)] > 0 {
			listStatuses[i] = make([]nodeProgress, 1)
		}
	}

	var eg errgroup.Group

	if inPolicy.PreferLocal && inPolicy.MaxReplicas > 0 {
		localLists := make([]int, 0, len(nodeLists))
		for i := range nodeLists {
			if localNodeInSet(t.placementIterator.neoFSNet, nodeLists[i]) {
				localLists = append(localLists, i)
			}
		}

		ok, err := t._handleInitialPlacementSeq(inPolicy.MaxReplicas, repLimits, ecLimits, mainECRules, nodeLists, obj, encObj, listStatuses, &eg, true, slices.All(localLists))
		if err != nil || ok == inPolicy.MaxReplicas {
			return err
		}

		inPolicy.MaxReplicas -= ok
	}

	_, err := t._handleInitialPlacementSeq(inPolicy.MaxReplicas, repLimits, ecLimits, mainECRules, nodeLists, obj, encObj, listStatuses, &eg, false, func(yield func(int, int) bool) {
		for i := range nodeLists {
			if !yield(i, i) {
				return
			}
		}
	})
	return err
}

func (t *distributedTarget) _handleInitialPlacementSeq(maxReplicas uint32, repLimits []uint32, ecLimits []uint32, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	obj object.Object, encObj encodedObject, listStatuses [][]nodeProgress, eg *errgroup.Group, local bool, listSeq iter.Seq2[int, int]) (uint32, error) {
	leftReplicas := maxReplicas
group:
	for {
		t._fillNextInitialPlacementGroup(leftReplicas, repLimits, ecLimits, ecRules, nodeLists, obj, encObj, listStatuses, eg, local, listSeq)

		if err := eg.Wait(); err != nil {
			return 0, err
		}

		for _, i := range listSeq {
			for j := range listStatuses[i] {
				if listStatuses[i][j].status == statusWIP {
					if listStatuses[i][j].ok {
						listStatuses[i][j].status = statusOK
					} else {
						listStatuses[i][j].status = statusFail
					}
				}
			}
		}

		if maxReplicas > 0 {
			var ok, undone uint32
			for _, i := range listSeq {
				var limit uint32
				if i < len(repLimits) {
					limit = repLimits[i]
				} else {
					limit = ecLimits[i]
				}

				if limit == 0 {
					continue
				}

				for j := range listStatuses[i] {
					switch listStatuses[i][j].status {
					case statusUndone:
						undone++
					case statusOK:
						if ok++; ok == maxReplicas {
							return ok, nil
						}
					case statusFail:
					default:
						panic(fmt.Sprintf("unexpected enum value %d", listStatuses[i][j].status))
					}
				}
			}

			if local {
				if undone == 0 {
					return ok, nil
				}
				continue
			}

			if maxReplicas <= ok+undone {
				leftReplicas = maxReplicas - ok
				continue
			}

			err := fmt.Errorf("unable to reach MaxReplicas %d (succeeded: %d, left nodes: %d)", maxReplicas, ok, undone)
			if ok == 0 {
				return 0, err
			}
			return 0, wrapIncompleteError(err)
		}

	nextList:
		for i := range listStatuses {
			var limit uint32
			if i < len(repLimits) {
				limit = repLimits[i]
			} else {
				limit = ecLimits[i]
			}

			if limit == 0 {
				continue
			}

			var ok, undone uint32
			for j := range listStatuses[i] {
				switch listStatuses[i][j].status {
				case statusUndone:
					undone++
				case statusOK:
					if ok++; ok == limit {
						continue nextList
					}
				case statusFail:
				default:
					panic(fmt.Sprintf("unexpected enum value %d", listStatuses[i][j].status))
				}
			}

			if limit <= ok+undone {
				continue group
			}

			err := fmt.Errorf("unable to reach replica number for rule #%d (required: %d, succeeded: %d, left nodes: %d)", i, limit, ok, undone)
			if ok == 0 {
				return 0, err
			}
			return 0, wrapIncompleteError(err)
		}

		return 0, nil
	}
}

func (t *distributedTarget) _fillNextInitialPlacementGroup(maxReplicas uint32, repLimits []uint32, ecLimits []uint32, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	obj object.Object, encObj encodedObject, listStatuses [][]nodeProgress, eg *errgroup.Group, local bool, listSeq iter.Seq2[int, int]) {
	var added uint32
	for {
		var extended bool
	nextList:
		for _, listIdx := range listSeq {
			var limit uint32
			ecRuleIdx := -1
			if listIdx >= len(repLimits) {
				ecRuleIdx = listIdx - len(repLimits)
				limit = ecLimits[ecRuleIdx]
			} else {
				limit = repLimits[listIdx]
			}

			if limit == 0 {
				continue
			}

			nodeIdx := -1
			var ok, wip uint32
		statusLoop:
			for i := range listStatuses[listIdx] {
				switch listStatuses[listIdx][i].status {
				case statusUndone:
					nodeIdx = i
					break statusLoop
				case statusOK:
					ok++
					if ok+wip == limit {
						continue nextList
					}
				case statusWIP:
					wip++
					if ok+wip == limit {
						continue nextList
					}
				case statusFail:
				default:
					panic(fmt.Sprintf("unexpected enum value %d", listStatuses[listIdx][i].status))
				}
			}

			if nodeIdx < 0 {
				continue
			}

			if ecRuleIdx >= 0 {
				eg.Go(func() error {
					_, err := t.ecAndSaveObjectByRule(t.sessionSigner, obj, ecRuleIdx, ecRules[ecRuleIdx], nodeLists[listIdx])
					if err != nil {
						t.placementIterator.log.Error("initial EC placement failed",
							zap.Stringer("object", obj.Address()), zap.Error(err)) // error contains rule info
					}
					listStatuses[listIdx][0].ok = err == nil
					if local {
						err = nil
					} // otherwise EC rule must succeed
					return err
				})

				listStatuses[listIdx][0].status = statusWIP

				if added++; added == maxReplicas {
					return
				}

				extended = true
				continue
			}

			var node nodeDesc
			node.local = t.placementIterator.neoFSNet.IsLocalNodePublicKey(nodeLists[listIdx][nodeIdx].PublicKey())
			node.placementVector = listIdx
			if !node.local {
				var err error
				node.info, err = convertNodeInfo(nodeLists[listIdx][nodeIdx])
				if err != nil {
					// https://github.com/nspcc-dev/neofs-node/issues/3565
					logNodeConversionError(t.placementIterator.log, nodeLists[listIdx][nodeIdx], err)
					setNodeStatus(listStatuses[:len(repLimits)], nodeLists[:len(repLimits)], listIdx, nodeIdx, statusFail)
					continue
				}
			}

			setNodeStatus(listStatuses[:len(repLimits)], nodeLists[:len(repLimits)], listIdx, nodeIdx, statusWIP)

			eg.Go(func() error {
				err := t.sendObject(obj, encObj, node, &t.metaCollection)
				if err != nil {
					t.placementIterator.log.Error("initial REP placement to node failed",
						zap.Stringer("object", obj.Address()), zap.Int("repRule", listIdx), zap.Int("nodeIdx", nodeIdx),
						zap.Bool("local", node.local), zap.Error(err)) // error contains addresses if remote
					markNodeFailure(listStatuses[:len(repLimits)], nodeLists[:len(repLimits)], listIdx, nodeIdx)
					// TODO: this could have been the last chance to comply with the rule. If it is
					//  missed, the entire operation should be aborted asap. Currently, if some other
					//  worker handles slow node, request handler will wait for it delaying the response.
					//  Note that this is relevant if local is unset only.
					return nil
				}
				markNodeSuccess(listStatuses[:len(repLimits)], nodeLists[:len(repLimits)], listIdx, nodeIdx)
				return nil
			})

			if added++; added == maxReplicas {
				return
			}

			extended = true
		}

		if !extended {
			return
		}
	}
}

func setNodeStatus(listStatuses [][]nodeProgress, nodeLists [][]netmap.NodeInfo, listIdx int, nodeIdx int, st progressStatus) {
	for i := range listStatuses {
		if i != listIdx {
			if ind := nodeIndexInSet(nodeLists[listIdx][nodeIdx], nodeLists[i]); ind >= 0 {
				listStatuses[i][ind].status = st
			}
		}
	}
	listStatuses[listIdx][nodeIdx].status = st
}

func markNodeSuccess(listStatuses [][]nodeProgress, nodeLists [][]netmap.NodeInfo, listIdx int, nodeIdx int) {
	_markNodeResult(listStatuses, nodeLists, listIdx, nodeIdx, true)
}

func markNodeFailure(listStatuses [][]nodeProgress, nodeLists [][]netmap.NodeInfo, listIdx int, nodeIdx int) {
	_markNodeResult(listStatuses, nodeLists, listIdx, nodeIdx, false)
}

func _markNodeResult(listStatuses [][]nodeProgress, nodeLists [][]netmap.NodeInfo, listIdx int, nodeIdx int, ok bool) {
	for i := range listStatuses {
		if i != listIdx {
			if ind := nodeIndexInSet(nodeLists[listIdx][nodeIdx], nodeLists[i]); ind >= 0 {
				listStatuses[i][ind].ok = ok
			}
		}
	}
	listStatuses[listIdx][nodeIdx].ok = ok
}
