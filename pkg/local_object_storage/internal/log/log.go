package storagelog

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// headMsg is a distinctive part of all messages.
const headMsg = "local object storage operation"

// Write writes message about storage engine's operation to logger.
func Write(logger *zap.Logger, fields ...zap.Field) {
	logger.Info(headMsg, fields...)
}

// AddressField returns logger's field for object address.
func AddressField(addr oid.Address) zap.Field {
	return zap.Stringer("address", addr)
}

// OpField returns logger's field for operation type.
func OpField(op string) zap.Field {
	return zap.String("op", op)
}

// StorageTypeField returns logger's field for storage type.
func StorageTypeField(typ string) zap.Field {
	return zap.String("type", typ)
}
