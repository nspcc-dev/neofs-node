package control

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
