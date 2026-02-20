package putsvc

import (
	"fmt"
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

type progress struct {
	status progressStatus
	ok     bool
}

func (t *distributedTarget) putInitial(obj object.Object, encObj encodedObject, repRules []uint, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo, policy InitialPlacementPolicy) error {
	var repLimits, ecLimits []uint32
	if policy.MaxPerRuleReplicas != nil {
		repLimits = policy.MaxPerRuleReplicas[:len(repRules)]
		ecLimits = policy.MaxPerRuleReplicas[len(repRules):]
	} else {
		// TODO: sync integer type with ContainerNodes.PrimaryCounts() to just assign here
		repLimits = make([]uint32, len(repRules))
		for i := range repLimits {
			repLimits[i] = uint32(repRules[i])
		}
		ecLimits = islices.RepeatElement(len(ecRules), uint32(1))
	}

	var repProg [][]progress
	if slices.ContainsFunc(repLimits, func(l uint32) bool { return l > 0 }) {
		repProg = make([][]progress, len(repLimits))
		for i := range repLimits {
			if repLimits[i] > 0 {
				repProg[i] = make([]progress, len(nodeLists[i]))
			}
		}
	}

	var ecProg []progress
	if slices.ContainsFunc(ecLimits, func(l uint32) bool { return l > 0 }) {
		ecProg = make([]progress, len(ecLimits))
	}

	var eg errgroup.Group

	if policy.MaxReplicas > 0 {
		return t.handleFlatInitialPlacementRule(obj, encObj, ecRules, nodeLists, policy.MaxReplicas, policy.PreferLocal, repLimits, ecLimits, repProg, ecProg, &eg)
	}

	return t.handleIndependentInitialPlacementRules(obj, encObj, ecRules, nodeLists, repLimits, ecLimits, repProg, ecProg, &eg)
}

func (t *distributedTarget) handleIndependentInitialPlacementRules(obj object.Object, encObj encodedObject, ecRules []iec.Rule,
	nodeLists [][]netmap.NodeInfo, repLimits []uint32, ecLimits []uint32, repProg [][]progress, ecProg []progress, eg *errgroup.Group) error {
	for {
		t.groupIndependentInitialPlacement(eg, obj, encObj, ecRules, nodeLists, repLimits, repProg, ecLimits, ecProg)

		if err := eg.Wait(); err != nil {
			return err
		}

		doneREP, err := checkIndependentREPProgress(repLimits, repProg)
		if err != nil {
			return err
		}

		doneEC := checkIndependentECProgress(ecLimits, ecProg)

		if doneREP && doneEC {
			return nil
		}
	}
}

func (t *distributedTarget) handleFlatInitialPlacementRule(obj object.Object, encObj encodedObject, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	maxReplicas uint32, preferLocal bool, repLimits []uint32, ecLimits []uint32, repProg [][]progress, ecProg []progress, eg *errgroup.Group) error {
	left := maxReplicas
	var err error
	for {
		preferLocal = t.groupFlatInitialPlacement(eg, obj, encObj, ecRules, nodeLists, maxReplicas, preferLocal, repLimits, repProg, ecLimits, ecProg)

		if err = eg.Wait(); err != nil {
			return err
		}

		left, err = checkFlatInitialPlacementProgress(maxReplicas, repLimits, ecLimits, repProg, ecProg)
		if left == 0 {
			return err
		}
	}
}

func (t *distributedTarget) groupIndependentInitialPlacement(eg *errgroup.Group, obj object.Object, encObj encodedObject, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	repLimits []uint32, repProg [][]progress, ecLimits []uint32, ecProg []progress) {
	t.groupIndependentInitialREP(eg, obj, encObj, nodeLists[:len(repLimits)], repLimits, repProg)
	t.groupIndependentInitialEC(eg, obj, ecRules, nodeLists[len(repLimits):], ecLimits, ecProg, true)
}

func (t *distributedTarget) groupIndependentInitialREP(eg *errgroup.Group, obj object.Object, encObj encodedObject, nodeLists [][]netmap.NodeInfo,
	limits []uint32, prog [][]progress) {
nextRule:
	for ruleIdx := range limits {
		if limits[ruleIdx] == 0 {
			continue
		}

		var okOrWIP uint32
		for nodeIdx := range prog[ruleIdx] {
			switch prog[ruleIdx][nodeIdx].status {
			case statusUndone:
				if !t.startInitialREPToNode(eg, obj, encObj, nodeLists, prog, ruleIdx, nodeIdx) {
					setNodeStatus(prog, nodeLists, ruleIdx, nodeIdx, statusFail)
					break
				}
				setNodeStatus(prog, nodeLists, ruleIdx, nodeIdx, statusWIP)
				fallthrough
			case statusOK, statusWIP:
				if okOrWIP++; okOrWIP == limits[ruleIdx] {
					continue nextRule
				}
			case statusFail:
			default:
				panic(fmt.Sprintf("unexpected enum value %d", prog[ruleIdx][nodeIdx].status))
			}
		}
	}
}

func (t *distributedTarget) groupIndependentInitialEC(eg *errgroup.Group, obj object.Object, rules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	limits []uint32, prog []progress, mustSucceed bool) {
	for ruleIdx := range limits {
		if limits[ruleIdx] == 0 {
			continue
		}

		switch prog[ruleIdx].status {
		case statusUndone:
			t.startInitialECByRule(eg, obj, nodeLists[ruleIdx], rules[ruleIdx], prog, ruleIdx, mustSucceed)
			prog[ruleIdx].status = statusWIP
		case statusOK, statusWIP, statusFail:
		default:
			panic(fmt.Sprintf("unexpected enum value %d", prog[ruleIdx].status))
		}
	}
}

func (t *distributedTarget) groupFlatInitialPlacement(eg *errgroup.Group, obj object.Object, encObj encodedObject, ecRules []iec.Rule, nodeLists [][]netmap.NodeInfo,
	limit uint32, localOnly bool, repLimits []uint32, repProg [][]progress, ecLimits []uint32, ecProg []progress) bool {
	for {
		var added bool
	nextRule:
		for ruleIdx := range repLimits {
			if repLimits[ruleIdx] == 0 {
				continue
			}

			if localOnly && !localNodeInSet(t.placementIterator.neoFSNet, nodeLists[ruleIdx]) {
				continue
			}

			var okOrWIP uint32
			for nodeIdx := range repProg[ruleIdx] {
				switch repProg[ruleIdx][nodeIdx].status {
				case statusUndone:
					if !t.startInitialREPToNode(eg, obj, encObj, nodeLists, repProg, ruleIdx, nodeIdx) {
						setNodeStatus(repProg, nodeLists, ruleIdx, nodeIdx, statusFail)
						break
					}
					setNodeStatus(repProg, nodeLists, ruleIdx, nodeIdx, statusWIP)
					if limit--; limit == 0 {
						return localOnly
					}
					added = true
					fallthrough
				case statusOK, statusWIP:
					if okOrWIP++; okOrWIP == repLimits[ruleIdx] {
						continue nextRule
					}
				case statusFail:
				default:
					panic(fmt.Sprintf("unexpected enum value %d", repProg[ruleIdx][nodeIdx].status))
				}
			}
		}

		for ruleIdx := range ecLimits {
			if ecLimits[ruleIdx] == 0 {
				continue
			}

			listIdx := len(repLimits) + ruleIdx
			if localOnly && !localNodeInSet(t.placementIterator.neoFSNet, nodeLists[listIdx]) {
				continue
			}

			switch ecProg[ruleIdx].status {
			case statusUndone:
				t.startInitialECByRule(eg, obj, nodeLists[listIdx], ecRules[ruleIdx], ecProg, ruleIdx, false)
				ecProg[ruleIdx].status = statusWIP
				if limit--; limit == 0 {
					return localOnly
				}
				added = true
			case statusOK, statusWIP, statusFail:
			default:
				panic(fmt.Sprintf("unexpected enum value %d", ecProg[ruleIdx].status))
			}
		}

		if !added {
			if !localOnly {
				return false
			}
			localOnly = false
		}
	}
}

func (t *distributedTarget) startInitialREPToNode(eg *errgroup.Group, obj object.Object, encObj encodedObject,
	nodeLists [][]netmap.NodeInfo, prog [][]progress, ruleIdx, nodeIdx int) bool {
	var node nodeDesc
	node.local = t.placementIterator.neoFSNet.IsLocalNodePublicKey(nodeLists[ruleIdx][nodeIdx].PublicKey())
	if !node.local {
		var err error
		node.info, err = convertNodeInfo(nodeLists[ruleIdx][nodeIdx])
		if err != nil {
			// https://github.com/nspcc-dev/neofs-node/issues/3565
			logNodeConversionError(t.placementIterator.log, nodeLists[ruleIdx][nodeIdx], err)
			return false
		}
	}

	node.placementVector = ruleIdx

	eg.Go(func() error {
		err := t.sendObject(obj, encObj, node, &t.metaCollection)
		if err != nil {
			t.placementIterator.log.Error("initial REP placement to node failed",
				zap.Stringer("object", obj.Address()), zap.Int("repRule", ruleIdx), zap.Int("nodeIdx", nodeIdx),
				zap.Bool("local", node.local), zap.Error(err)) // error contains addresses if remote

			// TODO: this could have been the last chance to comply with the rule. If it is
			//  missed, the entire operation should be aborted asap. Currently, if some other
			//  worker handles slow node, request handler will wait for it delaying the response.
			return nil
		}

		markNodeSuccess(prog, nodeLists, ruleIdx, nodeIdx)

		return nil
	})

	return true
}

func (t *distributedTarget) startInitialECByRule(eg *errgroup.Group, obj object.Object, nodeList []netmap.NodeInfo, rule iec.Rule,
	prog []progress, ruleIdx int, mustSucceed bool) {
	eg.Go(func() error {
		_, err := t.ecAndSaveObjectByRule(t.sessionSigner, obj, ruleIdx, rule, nodeList)
		if err != nil {
			t.placementIterator.log.Error("initial EC placement failed",
				zap.Stringer("object", obj.Address()), zap.Error(err)) // error contains rule info

			prog[ruleIdx].ok = false

			if mustSucceed {
				return err
			}
			return nil
		}

		prog[ruleIdx].ok = true

		return nil
	})
}

func setNodeStatus(listStatuses [][]progress, nodeLists [][]netmap.NodeInfo, ruleIdx int, nodeIdx int, st progressStatus) {
	for i := range listStatuses {
		if i != ruleIdx {
			if ind := nodeIndexInSet(nodeLists[ruleIdx][nodeIdx], nodeLists[i]); ind >= 0 {
				listStatuses[i][ind].status = st
			}
		}
	}
	listStatuses[ruleIdx][nodeIdx].status = st
}

func markNodeSuccess(listStatuses [][]progress, nodeLists [][]netmap.NodeInfo, ruleIdx int, nodeIdx int) {
	for i := range listStatuses {
		if i != ruleIdx {
			if ind := nodeIndexInSet(nodeLists[ruleIdx][nodeIdx], nodeLists[i]); ind >= 0 {
				listStatuses[i][ind].ok = true
			}
		}
	}
	listStatuses[ruleIdx][nodeIdx].ok = true
}

func checkIndependentREPProgress(limits []uint32, prog [][]progress) (bool, error) {
	done := true

nextRule:
	for ruleIdx := range limits {
		if limits[ruleIdx] == 0 {
			continue
		}

		var ok, undone uint32
		for nodeIdx := range prog[ruleIdx] {
			if prog[ruleIdx][nodeIdx].status == statusWIP {
				if prog[ruleIdx][nodeIdx].ok {
					prog[ruleIdx][nodeIdx].status = statusOK
				} else {
					prog[ruleIdx][nodeIdx].status = statusFail
				}
			}

			switch prog[ruleIdx][nodeIdx].status {
			case statusUndone:
				undone++
			case statusOK:
				if ok++; ok == limits[ruleIdx] && undone == 0 {
					continue nextRule
				}
			case statusFail:
			default:
				panic(fmt.Sprintf("unexpected enum value %d", prog[ruleIdx][nodeIdx].status))
			}
		}

		if limits[ruleIdx] <= ok+undone {
			done = false
			continue
		}

		err := fmt.Errorf("unable to reach REP rule #%d (required: %d, succeeded: %d, left nodes: %d)",
			ruleIdx, limits[ruleIdx], ok, undone)
		if ok == 0 {
			return false, err
		}
		return false, wrapIncompleteError(err)
	}

	return done, nil
}

func checkIndependentECProgress(limits []uint32, prog []progress) bool {
	done := true

	for ruleIdx := range limits {
		if limits[ruleIdx] == 0 {
			continue
		}

		if prog[ruleIdx].status == statusWIP {
			if !prog[ruleIdx].ok {
				// must have already been caught by errgroup
				panic("unexpected failed EC rule")
			}
			prog[ruleIdx].status = statusOK
		}

		switch prog[ruleIdx].status {
		case statusUndone:
			done = false
		case statusOK:
		default:
			panic(fmt.Sprintf("unexpected enum value %d", prog[ruleIdx].status))
		}
	}

	return done
}

func checkFlatInitialPlacementProgress(maxReplicas uint32, repLimits []uint32, ecLimits []uint32, repProg [][]progress, ecProg []progress) (uint32, error) {
	var okTotal, undoneTotal uint32

nextRule:
	for ruleIdx := range repLimits {
		if repLimits[ruleIdx] == 0 {
			continue
		}

		var ok, undone uint32
		for nodeIdx := range repProg[ruleIdx] {
			if repProg[ruleIdx][nodeIdx].status == statusWIP {
				if repProg[ruleIdx][nodeIdx].ok {
					repProg[ruleIdx][nodeIdx].status = statusOK
				} else {
					repProg[ruleIdx][nodeIdx].status = statusFail
				}
			}

			switch repProg[ruleIdx][nodeIdx].status {
			case statusUndone:
				undone++
				undoneTotal++
			case statusOK:
				if okTotal++; okTotal == maxReplicas {
					return 0, nil
				}
				if ok++; ok == repLimits[ruleIdx] && undone == 0 {
					continue nextRule
				}
			case statusFail:
			default:
				panic(fmt.Sprintf("unexpected enum value %d", repProg[ruleIdx][nodeIdx].status))
			}
		}
	}

	for ruleIdx := range ecLimits {
		if ecLimits[ruleIdx] == 0 {
			continue
		}

		if ecProg[ruleIdx].status == statusWIP {
			if ecProg[ruleIdx].ok {
				ecProg[ruleIdx].status = statusOK
			} else {
				ecProg[ruleIdx].status = statusFail
			}
		}

		switch ecProg[ruleIdx].status {
		case statusUndone:
			undoneTotal++
		case statusOK:
			if okTotal++; okTotal == maxReplicas {
				return 0, nil
			}
		case statusFail:
		default:
			panic(fmt.Sprintf("unexpected enum value %d", ecProg[ruleIdx].status))
		}
	}

	if maxReplicas > undoneTotal {
		err := fmt.Errorf("unable to reach MaxReplicas %d (succeeded: %d, left nodes: %d)", maxReplicas, okTotal, undoneTotal)
		if okTotal == 0 {
			return 0, err
		}
		return 0, wrapIncompleteError(err)
	}

	return maxReplicas - okTotal, nil
}
