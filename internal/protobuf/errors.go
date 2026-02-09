package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// NewUnorderedFieldsError returns common error for field order violation when
// field #n2 goes after #n1.
func NewUnorderedFieldsError(n1, n2 protowire.Number) error {
	return fmt.Errorf("unordered fields: #%d after #%d", n2, n1)
}
