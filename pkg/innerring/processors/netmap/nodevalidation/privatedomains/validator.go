package privatedomains

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// ErrMissingDomainRecord is returned when some record is missing in the
// particular domain.
var ErrMissingDomainRecord = errors.New("missing domain record")

// NNS provides services of the NeoFS NNS consumed by [Validator] to process.
type NNS interface {
	// CheckDomainRecord checks whether NNS domain with the specified name exists
	// and has given TXT record. Returns [ErrMissingDomainRecord] if domain exists
	// but has no given record, or any other error encountered prevented the check.
	//
	// Both domain name and record are non-empty.
	CheckDomainRecord(domainName string, record string) error
}

// Validator validates NNS domains declared by the storage nodes on their
// attempts to enter the NeoFS network map.
//
// There is an option to specify name of the verified nodes' domain. Such
// domains allow to combine several nodes into a private group (kind of subnet).
// Access is controlled using access lists: Validator checks that any incoming
// node declaring private node domain is presented in the corresponding access
// list. Access lists are stored in the NeoFS NNS: for each private node group,
// there is a registered NNS domain. TXT records of each such domain are Neo
// addresses of the nodes' public keys. To be allowed to use a specific verified
// domain value, the storage node must have a Neo address from this list.
// Otherwise, the storage node will be denied access to the network map. Note
// that if domain exists but has no records, then access is forbidden for
// anyone.
type Validator struct {
	nns NNS
}

// New returns new Validator that uses provided [NNS] as a source of node access
// records.
func New(nns NNS) *Validator {
	return &Validator{
		nns: nns,
	}
}

// various errors useful for testing.
var (
	errMissingNodeBinaryKey = errors.New("missing node binary key")
	errAccessDenied         = errors.New("access denied")
)

// VerifyAndUpdate checks allowance of the storage node represented by the given
// descriptor to enter the private node group (if any). Returns an error if on
// access denial or the check cannot be done at the moment.
//
// VerifyAndUpdate does not mutate the argument.
func (x *Validator) Verify(info netmap.NodeInfo) error {
	verifiedNodesDomain := info.VerifiedNodesDomain()
	if verifiedNodesDomain == "" {
		return nil
	}

	bNodeKey := info.PublicKey()
	if len(bNodeKey) == 0 {
		return errMissingNodeBinaryKey
	}

	buf := io.NewBufBinWriter()
	emit.CheckSig(buf.BinWriter, bNodeKey)
	nodeNeoAddress := address.Uint160ToString(hash.Hash160(buf.Bytes()))

	// record format is described in NEP-18 Specification https://github.com/neo-project/proposals/pull/133
	err := x.nns.CheckDomainRecord(verifiedNodesDomain, "address="+nodeNeoAddress)
	if err != nil {
		if errors.Is(err, ErrMissingDomainRecord) {
			return errAccessDenied
		}

		return fmt.Errorf("check records of the domain %q: %w", verifiedNodesDomain, err)
	}

	return nil
}
