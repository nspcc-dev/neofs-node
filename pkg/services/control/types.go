package control

import (
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

// SetPublicKey sets public key of the NeoFS node in a binary format.
func (x *NodeInfo) SetPublicKey(v []byte) {
	if x != nil {
		x.PublicKey = v
	}
}

// SetAddresses sets ways to connect to a node.
func (x *NodeInfo) SetAddresses(v []string) {
	if x != nil {
		x.Addresses = v
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

func (x *Netmap) MarshalJSON() ([]byte, error) {
	return protojson.MarshalOptions{
		EmitUnpopulated: true,
	}.Marshal(x)
}

// SetID sets identificator of the shard.
func (x *ShardInfo) SetID(v []byte) {
	x.Shard_ID = v
}

// SetMetabasePath sets path to shard's metabase.
func (x *ShardInfo) SetMetabasePath(v string) {
	x.MetabasePath = v
}

// SetWriteCachePath sets path to shard's write-cache.
func (x *ShardInfo) SetWriteCachePath(v string) {
	x.WritecachePath = v
}

// SetMode sets path to shard's work mode.
func (x *ShardInfo) SetMode(v ShardMode) {
	x.Mode = v
}

// SetErrorCount sets shard's error counter.
func (x *ShardInfo) SetErrorCount(count uint32) {
	x.ErrorCount = count
}
