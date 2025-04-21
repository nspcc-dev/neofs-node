package storagelog

import (
	"go.uber.org/zap"
)

// headMsg is a distinctive part of all messages.
const headMsg = "local object storage operation"

// Write writes message about storage engine's operation to logger.
func Write(logger *zap.Logger, fields ...zap.Field) {
	logger.Info(headMsg, fields...)
}

// AddressField returns logger's field for object address.
//
// Address should be type of *object.Address or string.
func AddressField(addr any) zap.Field {
	return zap.Any("address", addr)
}

// OpField returns logger's field for operation type.
func OpField(op string) zap.Field {
	return zap.String("op", op)
}

// StorageTypeField returns logger's field for storage type.
func StorageTypeField(typ string) zap.Field {
	return zap.String("type", typ)
}
