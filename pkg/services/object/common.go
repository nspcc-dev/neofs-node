package object

import (
	"fmt"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// getStatusCodeFromResponseMetaHeader checks whether buf is a valid response
// meta header. If so, status code field is returned. In case of nesting
// headers, code from the root is returned.
//
// Absence of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed: if status field is repeated (including nested),
// code from the last one is returned.
func getStatusCodeFromResponseMetaHeader(buffers iprotobuf.BuffersSlice) (uint32, error) {
	var code uint32
	var gotOrigin bool

	var opts protoscan.ScanMessageOptions
	opts.InterceptNested = func(num protowire.Number, nested iprotobuf.BuffersSlice) error {
		switch num {
		default:
			return protoscan.ErrContinue
		case protosession.FieldResponseMetaHeaderOrigin:
			var err error
			code, err = getStatusCodeFromResponseMetaHeader(nested)
			if err != nil {
				return fmt.Errorf("handle origin field: %w", err)
			}
			gotOrigin = true
			return nil
		case protosession.FieldResponseMetaHeaderStatus:
			if gotOrigin {
				return protoscan.ErrContinue
			}
			var opts protoscan.ScanMessageOptions
			opts.InterceptUint32 = func(num protowire.Number, u uint32) error {
				if num == protostatus.FieldStatusCode {
					code = u
				}
				return nil
			}
			err := protoscan.ScanMessage(nested, protoscan.ResponseStatusScheme, opts)
			if err != nil {
				return fmt.Errorf("handle status field: %w", err)
			}
			return nil
		}
	}

	err := protoscan.ScanMessage(buffers, protoscan.ResponseMetaHeaderScheme, opts)
	if err != nil {
		return 0, err
	}

	return code, nil
}
