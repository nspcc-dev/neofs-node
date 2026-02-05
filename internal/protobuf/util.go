package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

func checkFieldType(num protowire.Number, exp, got protowire.Type) error {
	if exp == got {
		return nil
	}
	return fmt.Errorf("wrong type of field #%d: expected %v, got %v", num, exp, got)
}
