package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

var (
	_ interop.Contract = (*MetaData)(nil)

	// Hash is a hash of native MetaData contract.
	Hash = state.CreateNativeContractHash(MetaDataContractName)
)

// MetaData is a native contract for processing NeoFS objects meta data.
type MetaData struct {
	interop.ContractMD

	neo native.INEO
}

func (m *MetaData) Initialize(_ *interop.Context, _ *config.Hardfork, _ *interop.HFSpecificContractMD) error {
	return nil
}

func (m *MetaData) ActiveIn() *config.Hardfork {
	return nil
}

func (m *MetaData) InitializeCache(_ interop.IsHardforkEnabled, _ uint32, _ *dao.Simple) error {
	return nil
}

func (m *MetaData) Metadata() *interop.ContractMD {
	return &m.ContractMD
}

func (m *MetaData) OnPersist(_ *interop.Context) error {
	return nil
}

func (m *MetaData) PostPersist(_ *interop.Context) error {
	return nil
}

// NewMetadata returns native [MetaData] native contract.
func NewMetadata(neo native.INEO) *MetaData {
	m := &MetaData{neo: neo}
	defer m.BuildHFSpecificMD(m.ActiveIn())
	m.ContractMD = *interop.NewContractMD(MetaDataContractName, MetaDataContractID)

	desc := native.NewDescriptor("submitObjectPut", smartcontract.VoidType,
		manifest.NewParameter("metaInformation", smartcontract.ByteArrayType))
	md := native.NewMethodAndPrice(m.submitObjectPut, 1<<15, callflag.States|callflag.AllowNotify)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("registerMetaContainer", smartcontract.VoidType,
		manifest.NewParameter("container", smartcontract.Hash256Type))
	md = native.NewMethodAndPrice(m.registerMetaContainer, 1<<15, callflag.WriteStates)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("unregisterMetaContainer", smartcontract.VoidType,
		manifest.NewParameter("container", smartcontract.Hash256Type))
	md = native.NewMethodAndPrice(m.unregisterMetaContainer, 1<<15, callflag.WriteStates)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("updateContainerList", smartcontract.VoidType,
		manifest.NewParameter("container", smartcontract.Hash256Type),
		manifest.NewParameter("vectors", smartcontract.ArrayType))
	md = native.NewMethodAndPrice(m.updateContainerList, 1<<15, callflag.WriteStates)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("verifyPlacementSignatures", smartcontract.BoolType,
		manifest.NewParameter("container", smartcontract.Hash256Type),
		manifest.NewParameter("signatures", smartcontract.ArrayType))
	md = native.NewMethodAndPrice(m.verifyPlacementSignatures, 1<<15, callflag.ReadOnly)
	m.AddMethod(md, desc)

	eDesc := native.NewEventDescriptor(putObjectEvent,
		manifest.NewParameter("container", smartcontract.Hash256Type),
		manifest.NewParameter("object", smartcontract.Hash256Type),
		manifest.NewParameter("meta", smartcontract.MapType),
	)
	eMD := native.NewEvent(eDesc)
	m.AddEvent(eMD)

	return m
}

func objectIDFromStackItem(i stackitem.Item) (util.Uint256, error) {
	return stackitem.ToUint256(i)
}
