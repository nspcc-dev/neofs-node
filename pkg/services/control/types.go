package control

// SetKey sets public key used for signing.
func (m *Signature) SetKey(v []byte) {
	if m != nil {
		m.Key = v
	}
}

// SetSign sets binary signature.
func (m *Signature) SetSign(v []byte) {
	if m != nil {
		m.Sign = v
	}
}

// SetKey sets key of the node attribute.
func (m *NodeInfo_Attribute) SetKey(v string) {
	if m != nil {
		m.Key = v
	}
}

// SetValue sets value of the node attribute.
func (m *NodeInfo_Attribute) SetValue(v string) {
	if m != nil {
		m.Value = v
	}
}

// SetParents sets parent keys.
func (m *NodeInfo_Attribute) SetParents(v []string) {
	if m != nil {
		m.Parents = v
	}
}

// SetPublicKey sets public key of the NeoFS node in a binary format.
func (m *NodeInfo) SetPublicKey(v []byte) {
	if m != nil {
		m.PublicKey = v
	}
}

// SetAddress sets ways to connect to a node.
func (m *NodeInfo) SetAddress(v string) {
	if m != nil {
		m.Address = v
	}
}

// SetAttributes sets attributes of the NeoFS Storage Node.
func (m *NodeInfo) SetAttributes(v []*NodeInfo_Attribute) {
	if m != nil {
		m.Attributes = v
	}
}

// SetState sets state of the NeoFS node.
func (m *NodeInfo) SetState(v HealthStatus) {
	if m != nil {
		m.State = v
	}
}

// SetEpoch sets revision number of the network map.
func (m *Netmap) SetEpoch(v uint64) {
	if m != nil {
		m.Epoch = v
	}
}

// SetNodes sets nodes presented in network.
func (m *Netmap) SetNodes(v []*NodeInfo) {
	if m != nil {
		m.Nodes = v
	}
}
