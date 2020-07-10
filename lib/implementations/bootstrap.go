package implementations

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
	"github.com/nspcc-dev/neofs-node/lib/boot"
	"github.com/nspcc-dev/neofs-node/lib/ir"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/pkg/errors"
)

// MorphNetmapContract is a wrapper over NeoFS Netmap contract client
// that provides an interface of network map manipulations.
type MorphNetmapContract struct {
	// NeoFS Netmap smart-contract
	netmapContract StaticContractClient

	// add peer method name of netmap contract
	addPeerMethodName string

	// new epoch method name of netmap contract
	newEpochMethodName string

	// get netmap method name of netmap contract
	getNetMapMethodName string

	// update state method name of netmap contract
	updStateMethodName string

	// IR list method name of netmap contract
	irListMethodName string
}

// UpdateEpochParams is a structure that groups the parameters
// for NeoFS epoch number updating.
type UpdateEpochParams struct {
	epoch uint64
}

// UpdateStateParams is a structure that groups the parameters
// for NeoFS node state updating.
type UpdateStateParams struct {
	st NodeState

	key []byte
}

// NodeState is a type of node states enumeration.
type NodeState int64

const (
	_ NodeState = iota

	// StateOffline is an offline node state value.
	StateOffline
)

const addPeerFixedArgNumber = 2

const nodeInfoFixedPrmNumber = 3

// SetNetmapContractClient is a Netmap contract client setter.
func (s *MorphNetmapContract) SetNetmapContractClient(v StaticContractClient) {
	s.netmapContract = v
}

// SetAddPeerMethodName is a Netmap contract AddPeer method name setter.
func (s *MorphNetmapContract) SetAddPeerMethodName(v string) {
	s.addPeerMethodName = v
}

// SetNewEpochMethodName is a Netmap contract NewEpoch method name setter.
func (s *MorphNetmapContract) SetNewEpochMethodName(v string) {
	s.newEpochMethodName = v
}

// SetNetMapMethodName is a Netmap contract Netmap method name setter.
func (s *MorphNetmapContract) SetNetMapMethodName(v string) {
	s.getNetMapMethodName = v
}

// SetUpdateStateMethodName is a Netmap contract UpdateState method name setter.
func (s *MorphNetmapContract) SetUpdateStateMethodName(v string) {
	s.updStateMethodName = v
}

// SetIRListMethodName is a Netmap contract InnerRingList method name setter.
func (s *MorphNetmapContract) SetIRListMethodName(v string) {
	s.irListMethodName = v
}

// AddPeer invokes the call of AddPeer method of NeoFS Netmap contract.
func (s *MorphNetmapContract) AddPeer(p boot.BootstrapPeerParams) error {
	info := p.NodeInfo()
	opts := info.GetOptions()

	args := make([]interface{}, 0, addPeerFixedArgNumber+len(opts))

	args = append(args,
		// Address
		[]byte(info.GetAddress()),

		// Public key
		info.GetPubKey(),
	)

	// Options
	for i := range opts {
		args = append(args, []byte(opts[i]))
	}

	return s.netmapContract.Invoke(
		s.addPeerMethodName,
		args...,
	)
}

// UpdateEpoch invokes the call of NewEpoch method of NeoFS Netmap contract.
func (s *MorphNetmapContract) UpdateEpoch(p UpdateEpochParams) error {
	return s.netmapContract.Invoke(
		s.newEpochMethodName,
		int64(p.Number()), // TODO: do not cast after uint64 type will become supported in client
	)
}

// GetNetMap performs the test invocation call of Netmap method of NeoFS Netmap contract.
func (s *MorphNetmapContract) GetNetMap(p netmap.GetParams) (*netmap.GetResult, error) {
	prms, err := s.netmapContract.TestInvoke(
		s.getNetMapMethodName,
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (Nodes): %d", ln)
	}

	prms, err = goclient.ArrayFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get stack item array from stack item (Nodes)")
	}

	nm := netmap.NewNetmap()

	for i := range prms {
		nodeInfo, err := nodeInfoFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse stack item (Node #%d)", i)
		}

		if err := nm.AddNode(nodeInfo); err != nil {
			return nil, errors.Wrapf(err, "could not add node #%d to network map", i)
		}
	}

	res := new(netmap.GetResult)
	res.SetNetMap(nm)

	return res, nil
}

func nodeInfoFromStackItem(prm smartcontract.Parameter) (*bootstrap.NodeInfo, error) {
	prms, err := goclient.ArrayFromStackParameter(prm)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array (NodeInfo)")
	} else if ln := len(prms); ln != nodeInfoFixedPrmNumber {
		return nil, errors.Errorf("unexpected stack item count (NodeInfo): expected %d, has %d", 3, ln)
	}

	res := new(bootstrap.NodeInfo)

	// Address
	addrBytes, err := goclient.BytesFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get byte array from stack item (Address)")
	}

	res.Address = string(addrBytes)

	// Public key
	res.PubKey, err = goclient.BytesFromStackParameter(prms[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get byte array from stack item (Public key)")
	}

	// Options
	prms, err = goclient.ArrayFromStackParameter(prms[2])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array (Options)")
	}

	res.Options = make([]string, 0, len(prms))

	for i := range prms {
		optBytes, err := goclient.BytesFromStackParameter(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get byte array from stack item (Option #%d)", i)
		}

		res.Options = append(res.Options, string(optBytes))
	}

	return res, nil
}

// UpdateState invokes the call of UpdateState method of NeoFS Netmap contract.
func (s *MorphNetmapContract) UpdateState(p UpdateStateParams) error {
	return s.netmapContract.Invoke(
		s.updStateMethodName,
		p.State().Int64(),
		p.Key(),
	)
}

// GetIRInfo performs the test invocation call of InnerRingList method of NeoFS Netmap contract.
func (s *MorphNetmapContract) GetIRInfo(ir.GetInfoParams) (*ir.GetInfoResult, error) {
	prms, err := s.netmapContract.TestInvoke(
		s.irListMethodName,
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (Nodes): %d", ln)
	}

	irInfo, err := irInfoFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get IR info from stack item")
	}

	res := new(ir.GetInfoResult)
	res.SetInfo(*irInfo)

	return res, nil
}

func irInfoFromStackItem(prm smartcontract.Parameter) (*ir.Info, error) {
	prms, err := goclient.ArrayFromStackParameter(prm)
	if err != nil {
		return nil, errors.Wrap(err, "could not get stack item array")
	}

	nodes := make([]ir.Node, 0, len(prms))

	for i := range prms {
		node, err := irNodeFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get node info from stack item (IRNode #%d)", i)
		}

		nodes = append(nodes, *node)
	}

	info := new(ir.Info)
	info.SetNodes(nodes)

	return info, nil
}

func irNodeFromStackItem(prm smartcontract.Parameter) (*ir.Node, error) {
	prms, err := goclient.ArrayFromStackParameter(prm)
	if err != nil {
		return nil, errors.Wrap(err, "could not get stack item array (IRNode)")
	}

	// Public key
	keyBytes, err := goclient.BytesFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get byte array from stack item (Key)")
	}

	node := new(ir.Node)
	node.SetKey(keyBytes)

	return node, nil
}

// SetNumber is an epoch number setter.
func (s *UpdateEpochParams) SetNumber(v uint64) {
	s.epoch = v
}

// Number is an epoch number getter.
func (s UpdateEpochParams) Number() uint64 {
	return s.epoch
}

// SetState is a state setter.
func (s *UpdateStateParams) SetState(v NodeState) {
	s.st = v
}

// State is a state getter.
func (s UpdateStateParams) State() NodeState {
	return s.st
}

// SetKey is a public key setter.
func (s *UpdateStateParams) SetKey(v []byte) {
	s.key = v
}

// Key is a public key getter.
func (s UpdateStateParams) Key() []byte {
	return s.key
}

// Int64 converts NodeState to int64.
func (s NodeState) Int64() int64 {
	return int64(s)
}
