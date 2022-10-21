package blobstor

import (
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const deleteOp = "DELETE"
const putOp = "PUT"

func logOp(l *logger.Logger, op string, addr oid.Address, typ string, sID []byte) {
	storagelog.Write(l,
		storagelog.AddressField(addr),
		storagelog.OpField(op),
		storagelog.StorageTypeField(typ),
		storagelog.StorageIDField(sID),
	)
}
