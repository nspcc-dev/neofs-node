package meta

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/common"
)

func (m *MetaData) unregisterMetaContainer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 1
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID := containerIDFromStackItem(args[0])

	ok := m.neo.CheckCommittee(ic)
	if !ok {
		panic(common.ErrAlphabetWitnessFailed)
	}

	ic.DAO.DeleteStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID...))

	return stackitem.Null{}
}

func (m *MetaData) registerMetaContainer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 1
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID := containerIDFromStackItem(args[0])

	ok := m.neo.CheckCommittee(ic)
	if !ok {
		panic(common.ErrAlphabetWitnessFailed)
	}

	ic.DAO.PutStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID...), state.StorageItem{})

	return stackitem.Null{}
}
