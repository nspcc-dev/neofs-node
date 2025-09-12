package fschaincontracts

// Various methods of FS chain Container contract.
const (
	CreateContainerMethod         = "create"
	RemoveContainerMethod         = "remove"
	PutContainerEACLMethod        = "putEACL"
	PutContainerReportMethod      = "putReport"
	IterateContainerReportsMethod = "iterateReports"
)

// CreateContainerParams are parameters of [CreateContainerMethod].
type CreateContainerParams struct {
	Container            []byte
	InvocationScript     []byte
	VerificationScript   []byte
	SessionToken         []byte
	DomainName           string
	DomainZone           string
	EnableObjectMetadata bool
}

// RemoveContainerParams are parameters of [RemoveContainerMethod].
type RemoveContainerParams struct {
	ID                 []byte
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte
}

// PutContainerEACLParams are parameters of [PutContainerEACLMethod].
type PutContainerEACLParams struct {
	EACL               []byte
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte
}
