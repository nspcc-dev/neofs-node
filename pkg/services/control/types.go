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
