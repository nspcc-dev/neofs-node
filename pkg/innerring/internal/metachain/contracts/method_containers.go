package contracts

import (
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"go.uber.org/zap"
)

func (m MetaData) removeMetaContainer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 1
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID, ok := args[0].Value().([]byte)
	if !ok {
		panic(fmt.Errorf("unexpected argument value: %T expected, %T given", cID, args[0].Value()))
	}
	if len(cID) != smartcontract.Hash256Len {
		panic(fmt.Errorf("unexpected container ID length: %d expected, %d given", smartcontract.Hash256Len, len(cID)))
	}

	err := m.checkCommitteeWitness(ic)
	if err != nil {
		panic(err)
	}

	ic.DAO.DeleteStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID...))
	logMessage(ic, "removed meta container", zap.String("cID", base58.Encode(cID)))

	return stackitem.Null{}
}

func (m MetaData) registerMetaContainer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 1
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID, ok := args[0].Value().([]byte)
	if !ok {
		panic(fmt.Errorf("unexpected argument value: %T expected, %T given", cID, args[0].Value()))
	}
	if len(cID) != smartcontract.Hash256Len {
		panic(fmt.Errorf("unexpected container ID length: %d expected, %d given", smartcontract.Hash256Len, len(cID)))
	}

	err := m.checkCommitteeWitness(ic)
	if err != nil {
		panic(err)
	}

	ic.DAO.PutStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID...), []byte{})
	logMessage(ic, "registered meta container", zap.String("cID", base58.Encode(cID)))

	return stackitem.Null{}
}
