package placement

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Placement stores storage policy application result for a particular
// container and network map.
type Placement struct {
	Epoch     uint64
	NodeSets  [][]netmap.NodeInfo
	RepCounts []uint
	ECRules   []iec.Rule
}

type cachedPlacement struct {
	Placement
	err error
}

type (
	containerNodesCacheKey struct {
		epoch uint64
		cnr   cid.ID
	}
	objectNodesCacheKey struct {
		epoch uint64
		addr  oid.Address
	}
)

const (
	// max number of container storage policy application results cached by
	// Service.
	cachedContainerNodesNum = 1000
	// max number of object storage policy application results cached by
	// Service.
	cachedObjectNodesNum = 10000
)

type (
	getContainerNodesFunc  = func(netmap.NetMap, netmap.PlacementPolicy, cid.ID) ([][]netmap.NodeInfo, error)
	sortContainerNodesFunc = func(netmap.NetMap, [][]netmap.NodeInfo, oid.ID) ([][]netmap.NodeInfo, error)
)

// Service applies container storage policies to NeoFS network maps and caches
// the results.
type Service struct {
	containers containercore.Source
	network    netmapcore.Source

	cache    *lru.Cache[containerNodesCacheKey, cachedPlacement]
	objCache *lru.Cache[objectNodesCacheKey, cachedPlacement]

	// for testing
	getContainerNodesFunc  getContainerNodesFunc
	sortContainerNodesFunc sortContainerNodesFunc
}

func New(containers containercore.Source, network netmapcore.Source) (*Service, error) {
	l, err := lru.New[containerNodesCacheKey, cachedPlacement](cachedContainerNodesNum)
	if err != nil {
		return nil, fmt.Errorf("create LRU container node cache for one epoch: %w", err)
	}
	lo, err := lru.New[objectNodesCacheKey, cachedPlacement](cachedObjectNodesNum)
	if err != nil {
		return nil, fmt.Errorf("create LRU container node cache for objects: %w", err)
	}
	return &Service{
		containers:             containers,
		network:                network,
		cache:                  l,
		objCache:               lo,
		getContainerNodesFunc:  netmap.NetMap.ContainerNodes,
		sortContainerNodesFunc: netmap.NetMap.PlacementVectors,
	}, nil
}

// ForEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
func (s *Service) ForEachContainerNodePublicKeyInLastTwoEpochs(cnrID cid.ID, f func(pubKey []byte) bool) error {
	return s.forEachContainerNode(cnrID, true, func(node netmap.NodeInfo) bool {
		return f(node.PublicKey())
	})
}

// SelectContainerNodes applies the storage policy of a specified container
// to the current network state and returns the result.
func (s *Service) SelectContainerNodes(cnrID cid.ID) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	_, curEpoch, resCur, err := s.selectContainerNodes(cnrID)
	if err != nil {
		return nil, nil, nil, err
	}

	if resCur.err != nil {
		return nil, nil, nil, fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, resCur.err)
	}

	return resCur.NodeSets, resCur.RepCounts, resCur.ECRules, nil
}

func (s *Service) selectContainerNodes(cnrID cid.ID) (containerPolicyContext, uint64, cachedPlacement, error) {
	curEpoch, err := s.network.Epoch()
	if err != nil {
		return containerPolicyContext{}, 0, cachedPlacement{}, fmt.Errorf("read current NeoFS epoch: %w", err)
	}

	cnrCtx := containerPolicyContext{id: cnrID, containers: s.containers, network: s.network, getNodesFunc: s.getContainerNodesFunc}

	resCur, err := cnrCtx.applyAtEpoch(curEpoch, s.cache)
	if err != nil {
		return containerPolicyContext{}, 0, cachedPlacement{}, fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, err)
	}

	return cnrCtx, curEpoch, resCur, nil
}

func (s *Service) forEachContainerNode(cnrID cid.ID, withPrevEpoch bool, f func(netmap.NodeInfo) bool) error {
	cnrCtx, curEpoch, resCur, err := s.selectContainerNodes(cnrID)
	if err != nil {
		return err
	}

	if resCur.err == nil { // error case handled below
		for i := range resCur.NodeSets {
			for j := range resCur.NodeSets[i] {
				if !f(resCur.NodeSets[i][j]) {
					return nil
				}
			}
		}
	}

	if !withPrevEpoch || curEpoch == 0 {
		if resCur.err != nil {
			return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, resCur.err)
		}
		return nil
	}

	resPrev, err := cnrCtx.applyAtEpoch(curEpoch-1, s.cache)
	if err != nil {
		if resCur.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (current#%d) %w; (previous#%d) %w",
				curEpoch, resCur.err, curEpoch-1, err)
		}
		return fmt.Errorf("select container nodes for previous epoch #%d: %w", curEpoch-1, err)
	} else if resPrev.err == nil { // error case handled below
		for i := range resPrev.NodeSets {
			for j := range resPrev.NodeSets[i] {
				if !f(resPrev.NodeSets[i][j]) {
					return nil
				}
			}
		}
	}

	if resCur.err != nil {
		if resPrev.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (current#%d) %w; (previous#%d) %w",
				curEpoch, resCur.err, curEpoch-1, resPrev.err)
		}
		return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, resCur.err)
	} else if resPrev.err != nil {
		return fmt.Errorf("select container nodes for previous epoch #%d: %w", curEpoch-1, resPrev.err)
	}
	return nil
}

// GetNodesForObject reads storage policy of the referenced container from the
// underlying container storage, reads network map at the specified epoch from
// the underlying storage, applies the storage policy to it and returns sorted
// lists of selected storage nodes along with the per-list numbers of primary
// object holders. Resulting slices must not be changed.
func (s *Service) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	curEpoch, err := s.network.Epoch()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	cacheKey := objectNodesCacheKey{curEpoch, addr}
	res, ok := s.objCache.Get(cacheKey)
	if ok {
		return res.NodeSets, res.RepCounts, res.ECRules, res.err
	}
	cnrRes, networkMap, err := s.getForEpoch(curEpoch, addr.Container())
	if err != nil {
		return nil, nil, nil, err
	}
	if networkMap == nil {
		if networkMap, err = s.network.GetNetMapByEpoch(curEpoch); err != nil {
			// non-persistent error => do not cache
			return nil, nil, nil, fmt.Errorf("read network map by epoch: %w", err)
		}
	}
	res.RepCounts = cnrRes.RepCounts
	res.ECRules = cnrRes.ECRules
	res.NodeSets, res.err = s.sortContainerNodesFunc(*networkMap, cnrRes.NodeSets, addr.Object())
	if res.err != nil {
		res.err = fmt.Errorf("sort container nodes for object: %w", res.err)
	}
	s.objCache.Add(cacheKey, res)
	return res.NodeSets, res.RepCounts, res.ECRules, res.err
}

// GetContainerPlacement returns container placement for the current epoch.
func (s *Service) GetContainerPlacement(cnrID cid.ID) (Placement, error) {
	curEpoch, err := s.network.Epoch()
	if err != nil {
		return Placement{}, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	res, _, err := s.getForEpoch(curEpoch, cnrID)
	if err != nil {
		return Placement{}, err
	}
	return res, nil
}

func (s *Service) getForEpoch(epoch uint64, cnr cid.ID) (Placement, *netmap.NetMap, error) {
	policy, networkMap, err := (&containerPolicyContext{
		id:           cnr,
		containers:   s.containers,
		network:      s.network,
		getNodesFunc: s.getContainerNodesFunc,
	}).applyToNetmap(epoch, s.cache)
	if err != nil || policy.err != nil {
		if err == nil {
			err = policy.err // cached in s.cache, no need to store in s.objCache
		}
		return Placement{}, nil, fmt.Errorf("select container nodes for current epoch #%d: %w", epoch, err)
	}
	return policy.Placement, networkMap, nil
}

// SortContainerPlacementForObject sorts provided current-epoch placement for an object.
func (s *Service) SortContainerPlacementForObject(cnrID cid.ID, p Placement, obj oid.ID) ([][]netmap.NodeInfo, error) {
	cacheKey := objectNodesCacheKey{epoch: p.Epoch}
	cacheKey.addr.SetContainer(cnrID)
	cacheKey.addr.SetObject(obj)
	res, ok := s.objCache.Get(cacheKey)
	if ok {
		return res.NodeSets, res.err
	}
	networkMap, err := s.network.GetNetMapByEpoch(p.Epoch)
	if err != nil {
		return nil, fmt.Errorf("read network map by epoch: %w", err)
	}
	res.Placement = p
	res.NodeSets, res.err = s.sortContainerNodesFunc(*networkMap, p.NodeSets, obj)
	if res.err != nil {
		res.err = fmt.Errorf("sort container nodes for object: %w", res.err)
	}
	s.objCache.Add(cacheKey, res)
	return res.NodeSets, res.err
}

// containerPolicyContext preserves context of storage policy processing for a particular container.
type containerPolicyContext struct {
	// static
	id           cid.ID
	containers   containercore.Source
	network      netmapcore.Source
	getNodesFunc getContainerNodesFunc
	// dynamic
	cnr *container.Container
}

// applyAtEpoch applies storage policy of container referenced by parameterized
// ID to the network map at the specified epoch. applyAtEpoch checks existing
// results in the cache and stores new results in it.
func (x *containerPolicyContext) applyAtEpoch(epoch uint64, cache *lru.Cache[containerNodesCacheKey, cachedPlacement]) (cachedPlacement, error) {
	res, _, err := x.applyToNetmap(epoch, cache)
	return res, err
}

// applyToNetmap applies storage policy of container referenced by parameterized
// ID to the network map at the specified epoch. applyAtEpoch checks existing
// results in the cache and stores new results in it. Network map is returned if
// it was requested, i.e. on cache miss only.
func (x *containerPolicyContext) applyToNetmap(epoch uint64, cache *lru.Cache[containerNodesCacheKey, cachedPlacement]) (cachedPlacement, *netmap.NetMap, error) {
	cacheKey := containerNodesCacheKey{epoch, x.id}
	if result, ok := cache.Get(cacheKey); ok {
		return result, nil, nil
	}
	var result cachedPlacement
	var err error
	if x.cnr == nil {
		cnr, err := x.containers.Get(x.id)
		if err != nil {
			// non-persistent error => do not cache
			return result, nil, fmt.Errorf("read container by ID: %w", err)
		}
		x.cnr = &cnr
	}
	networkMap, err := x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		// non-persistent error => do not cache
		return result, nil, fmt.Errorf("read network map by epoch: %w", err)
	}
	policy := x.cnr.PlacementPolicy()
	result.Epoch = epoch
	result.NodeSets, result.err = x.getNodesFunc(*networkMap, policy, x.id)
	if result.err == nil {
		// ContainerNodes should control following, but still better to double-check
		if result.err = checkPolicyApplicationResult(policy, result.NodeSets); result.err == nil {
			result.RepCounts = make([]uint, policy.NumberOfReplicas())
			for i := range result.RepCounts {
				result.RepCounts[i] = uint(policy.ReplicaNumberByIndex(i))
			}

			result.ECRules = convertECRules(policy.ECRules())
		}
	}
	cache.Add(cacheKey, result)
	return result, networkMap, nil
}

func checkPolicyApplicationResult(policy netmap.PlacementPolicy, nodeSets [][]netmap.NodeInfo) error {
	ecRules := policy.ECRules()
	repRules := policy.Replicas()
	if len(nodeSets) != len(repRules)+len(ecRules) {
		return fmt.Errorf("invalid result of container's storage policy application to the network map: "+
			"diff number of storage node sets (%d) and rules (%d)",
			len(nodeSets), len(repRules)+len(ecRules))
	}

	for i := range repRules {
		if exp := repRules[i].NumberOfObjects(); exp > uint32(len(nodeSets[i])) {
			return fmt.Errorf("invalid result of container's storage policy application to the network map: "+
				"invalid storage node set #%d: number of nodes (%d) is less than minimum required by REP rule (%d)",
				i, len(nodeSets[i]), exp)
		}
	}

	for i := range ecRules {
		n := len(nodeSets[len(repRules)+i])
		if exp := int(ecRules[i].DataPartNum() + ecRules[i].ParityPartNum()); exp > n {
			return fmt.Errorf("invalid result of container's storage policy application to the network map: "+
				"invalid storage node set #%d: number of nodes (%d) is less than minimum required by EC rule (%d)",
				i, n, exp)
		}
	}

	return nil
}

func convertECRules(from []netmap.ECRule) []iec.Rule {
	to := make([]iec.Rule, len(from))
	for i := range to {
		to[i].DataPartNum = uint8(from[i].DataPartNum())
		to[i].ParityPartNum = uint8(from[i].ParityPartNum())
	}
	return to
}
