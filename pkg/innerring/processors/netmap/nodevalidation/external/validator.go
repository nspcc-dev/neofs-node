package external

import (
	"net/http"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

// Validator is a utility that uses external validator to verify node
// structure.
//
// For the correct operation, the Validator must be created
// using the constructor (New). After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct {
	endpoint   string
	privateKey *keys.PrivateKey
	client     *http.Client
}

// New creates a new instance of the Validator.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New(endpoint string, privateKey *keys.PrivateKey) *Validator {
	return &Validator{
		endpoint:   endpoint,
		privateKey: privateKey,
		client:     &http.Client{},
	}
}
