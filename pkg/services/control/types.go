package control

import (
	"github.com/nspcc-dev/neofs-api-go/util/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// SetKey sets public key used for signing.
func (x *Signature) SetKey(v []byte) {
	if x != nil {
		x.Key = v
	}
}

// SetSign sets binary signature.
func (x *Signature) SetSign(v []byte) {
	if x != nil {
		x.Sign = v
	}
}

// SetKey sets key of the node attribute.
func (x *NodeInfo_Attribute) SetKey(v string) {
	if x != nil {
		x.Key = v
	}
}

// SetValue sets value of the node attribute.
func (x *NodeInfo_Attribute) SetValue(v string) {
	if x != nil {
		x.Value = v
	}
}

// SetParents sets parent keys.
func (x *NodeInfo_Attribute) SetParents(v []string) {
	if x != nil {
		x.Parents = v
	}
}

const (
	_ = iota
	nodeAttrKeyFNum
	nodeAttrValueFNum
	nodeAttrParentsFNum
)

// StableMarshal reads binary representation of node attribute
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo_Attribute) StableMarshal(buf []byte) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var (
		offset, n int
		err       error
	)

	n, err = proto.StringMarshal(nodeAttrKeyFNum, buf[offset:], x.Key)
	if err != nil {
		return nil, err
	}

	offset += n

	n, err = proto.StringMarshal(nodeAttrValueFNum, buf[offset:], x.Value)
	if err != nil {
		return nil, err
	}

	offset += n

	for i := range x.Parents {
		n, err = proto.StringMarshal(nodeAttrParentsFNum, buf[offset:], x.Parents[i])
		if err != nil {
			return nil, err
		}

		offset += n
	}

	return buf, nil
}
func (x *NodeInfo_Attribute) MarshalStream(s proto.Stream) (int, error) {
	if x == nil {
		return 0, nil
	}

	var (
		offset, n int
		err       error
	)

	n, err = s.StringMarshal(nodeAttrKeyFNum, x.Key)
	if err != nil {
		return offset + n, err
	}

	offset += n

	n, err = s.StringMarshal(nodeAttrValueFNum, x.Value)
	if err != nil {
		return offset + n, err
	}

	offset += n

	for i := range x.Parents {
		n, err = s.StringMarshal(nodeAttrParentsFNum, x.Parents[i])
		if err != nil {
			return offset + n, err
		}

		offset += n
	}

	return offset, nil
}

// StableSize returns binary size of node attribute
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo_Attribute) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.StringSize(nodeAttrKeyFNum, x.Key)
	size += proto.StringSize(nodeAttrValueFNum, x.Value)

	parents := x.GetParents()
	for i := range parents {
		size += proto.StringSize(nodeAttrParentsFNum, parents[i])
	}

	return size
}

// SetPublicKey sets public key of the NeoFS node in a binary format.
func (x *NodeInfo) SetPublicKey(v []byte) {
	if x != nil {
		x.PublicKey = v
	}
}

// SetAddress sets ways to connect to a node.
func (x *NodeInfo) SetAddress(v string) {
	if x != nil {
		x.Address = v
	}
}

// SetAttributes sets attributes of the NeoFS Storage Node.
func (x *NodeInfo) SetAttributes(v []*NodeInfo_Attribute) {
	if x != nil {
		x.Attributes = v
	}
}

// SetState sets state of the NeoFS node.
func (x *NodeInfo) SetState(v NetmapStatus) {
	if x != nil {
		x.State = v
	}
}

const (
	_ = iota
	nodePubKeyFNum
	nodeAddrFNum
	nodeAttrsFNum
	nodeStateFNum
)

// StableMarshal reads binary representation of node information
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo) StableMarshal(buf []byte) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var (
		offset, n int
		err       error
	)

	n, err = proto.BytesMarshal(nodePubKeyFNum, buf[offset:], x.PublicKey)
	if err != nil {
		return nil, err
	}

	offset += n

	n, err = proto.StringMarshal(nodeAddrFNum, buf[offset:], x.Address)
	if err != nil {
		return nil, err
	}

	offset += n

	for i := range x.Attributes {
		n, err = proto.NestedStructureMarshal(nodeAttrsFNum, buf[offset:], x.Attributes[i])
		if err != nil {
			return nil, err
		}

		offset += n
	}

	_, err = proto.EnumMarshal(nodeStateFNum, buf[offset:], int32(x.State))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (x *NodeInfo) MarshalStream(s proto.Stream) (int, error) {
	if x == nil {
		return 0, nil
	}

	var (
		offset, n int
		err       error
	)

	n, err = s.BytesMarshal(nodePubKeyFNum, x.PublicKey)
	if err != nil {
		return offset + n, err
	}

	offset += n

	n, err = s.StringMarshal(nodeAddrFNum, x.Address)
	if err != nil {
		return offset + n, err
	}

	offset += n

	for i := range x.Attributes {
		n, err = s.NestedStructureMarshal(nodeAttrsFNum, x.Attributes[i])
		if err != nil {
			return offset + n, err
		}

		offset += n
	}

	n, err = s.EnumMarshal(nodeStateFNum, int32(x.State))
	return offset + n, err
}

// StableSize returns binary size of node information
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(nodePubKeyFNum, x.PublicKey)
	size += proto.StringSize(nodeAddrFNum, x.Address)

	for i := range x.Attributes {
		size += proto.NestedStructureSize(nodeAttrsFNum, x.Attributes[i])
	}

	size += proto.EnumSize(nodeStateFNum, int32(x.State))

	return size
}

// SetEpoch sets revision number of the network map.
func (x *Netmap) SetEpoch(v uint64) {
	if x != nil {
		x.Epoch = v
	}
}

// SetNodes sets nodes presented in network.
func (x *Netmap) SetNodes(v []*NodeInfo) {
	if x != nil {
		x.Nodes = v
	}
}

const (
	_ = iota
	netmapEpochFNum
	netmapNodesFNum
)

// StableMarshal reads binary representation of netmap  in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *Netmap) StableMarshal(buf []byte) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var (
		offset, n int
		err       error
	)

	n, err = proto.UInt64Marshal(netmapEpochFNum, buf[offset:], x.Epoch)
	if err != nil {
		return nil, err
	}

	offset += n

	for i := range x.Nodes {
		n, err = proto.NestedStructureMarshal(netmapNodesFNum, buf[offset:], x.Nodes[i])
		if err != nil {
			return nil, err
		}

		offset += n
	}

	return buf, nil
}

func (x *Netmap) MarshalStream(s proto.Stream) (int, error) {
	if x == nil {
		return 0, nil
	}

	var (
		offset, n int
		err       error
	)

	n, err = s.UInt64Marshal(netmapEpochFNum, x.Epoch)
	if err != nil {
		return n, err
	}

	offset += n

	for i := range x.Nodes {
		n, err = s.NestedStructureMarshal(netmapNodesFNum, x.Nodes[i])
		if err != nil {
			return offset + n, err
		}

		offset += n
	}

	return offset, nil
}

// StableSize returns binary size of netmap  in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *Netmap) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.UInt64Size(netmapEpochFNum, x.Epoch)

	for i := range x.Nodes {
		size += proto.NestedStructureSize(netmapNodesFNum, x.Nodes[i])
	}

	return size
}

func (x *Netmap) MarshalJSON() ([]byte, error) {
	return protojson.MarshalOptions{
		EmitUnpopulated: true,
	}.Marshal(x)
}
