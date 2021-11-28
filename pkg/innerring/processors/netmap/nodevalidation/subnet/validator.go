package subnet

import (
	"errors"

	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
)

// Validator is an utility that verifies node subnet
// allowance.
//
// For correct operation, Validator must be created
// using the constructor (New). After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct {
	subnetClient *morphsubnet.Client
}

// Prm groups the required parameters of the Validator's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	SubnetClient *morphsubnet.Client
}

// New creates a new instance of the Validator.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New(prm Prm) (*Validator, error) {
	switch {
	case prm.SubnetClient == nil:
		return nil, errors.New("ir/nodeValidator: subnet client is not set")
	}

	return &Validator{
		subnetClient: prm.SubnetClient,
	}, nil
}
