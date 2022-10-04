package storagelog

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// headMsg is a distinctive part of all messages.
const headMsg = "local object storage operation"

// Write writes message about storage engine's operation to logger.
func Write(logger *logger.Logger, fields ...logger.Field) {
	logger.Info(headMsg, fields...)
}

// AddressField returns logger's field for object address.
//
// Address should be type of oid.Address or string.
func AddressField(addr interface{}) logger.Field {
	switch v := addr.(type) {
	default:
		panic(fmt.Sprintf("unexpected type %T", addr))
	case string:
		return logger.FieldString("address", v)
	case oid.Address:
		return logger.FieldStringer("address", v)
	case *oid.Address:
		return logger.FieldStringer("address", v)
	}
}

// OpField returns logger's field for operation type.
func OpField(op string) logger.Field {
	return logger.FieldString("op", op)
}
