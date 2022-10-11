package models

type ContainerCreationRequest struct {
	Container []byte
	Signature []byte
	PublicKey []byte
	Session   []byte
	Name      string
	Zone      string
}

type ContainerRemovalRequest struct {
	Container []byte
	Signature []byte
	Session   []byte
}

type SetContainerExtendedACLRequest struct {
	ExtendedACL []byte
	Signature   []byte
	PublicKey   []byte
	Session     []byte
}
