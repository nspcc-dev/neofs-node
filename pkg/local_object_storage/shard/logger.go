package shard

import (
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const deleteOp = "DELETE"
const putOp = "PUT"

func logOp(l *zap.Logger, op string, addr oid.Address) {
	storagelog.Write(l,
		storagelog.AddressField(addr),
		storagelog.OpField(op),
	)
}
