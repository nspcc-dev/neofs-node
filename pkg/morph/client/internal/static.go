package internal

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// StaticClient groups client.StaticClient type interface methods which are needed to be inherited
// by upper-level client instances.
type StaticClient interface {
	ContractAddress() util.Uint160
}
