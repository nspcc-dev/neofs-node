package contracts

import (
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/runtime"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"go.uber.org/zap"
)

var (
	_ interop.Contract = (*MetaData)(nil)
)

// TODO
type MetaData struct {
	interop.ContractMD

	neo native.INEO
}

func (m MetaData) Initialize(_ *interop.Context, _ *config.Hardfork, _ *interop.HFSpecificContractMD) error {
	return nil
}

func (m MetaData) ActiveIn() *config.Hardfork {
	return nil
}

func (m MetaData) InitializeCache(_ interop.IsHardforkEnabled, _ uint32, _ *dao.Simple) error {
	return nil
}

func (m MetaData) Metadata() *interop.ContractMD {
	return &m.ContractMD
}

func (m MetaData) OnPersist(_ *interop.Context) error {
	return nil
}

func (m MetaData) PostPersist(_ *interop.Context) error {
	return nil
}

// TODO
func MetaDataContract(neo native.INEO) *MetaData {
	m := &MetaData{neo: neo}
	defer m.BuildHFSpecificMD(m.ActiveIn())
	m.ContractMD = *interop.NewContractMD(MetaDataContractName, MetaDataContractID)

	desc := native.NewDescriptor("submitObjectPut", smartcontract.VoidType,
		manifest.NewParameter("metaInformation", smartcontract.ByteArrayType),
		manifest.NewParameter("signatures", smartcontract.ArrayType))
	md := native.NewMethodAndPrice(m.submitObjectPut, 0, callflag.States|callflag.AllowNotify)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("registerMetaContainer", smartcontract.VoidType,
		manifest.NewParameter("cID", smartcontract.Hash256Type))
	md = native.NewMethodAndPrice(m.registerMetaContainer, 0, callflag.WriteStates)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("removeMetaContainer", smartcontract.VoidType,
		manifest.NewParameter("cID", smartcontract.Hash256Type))
	md = native.NewMethodAndPrice(m.removeMetaContainer, 0, callflag.WriteStates)
	m.AddMethod(md, desc)

	desc = native.NewDescriptor("updateContainerList", smartcontract.VoidType,
		manifest.NewParameter("cID", smartcontract.Hash256Type),
		manifest.NewParameter("vectors", smartcontract.ArrayType))
	md = native.NewMethodAndPrice(m.updateContainerList, 0, callflag.WriteStates)
	m.AddMethod(md, desc)

	eDesc := native.NewEventDescriptor(putObjectEvent,
		manifest.NewParameter("ContainerID", smartcontract.Hash256Type),
		manifest.NewParameter("ObjectID", smartcontract.Hash256Type),
		manifest.NewParameter("Meta", smartcontract.MapType),
	)
	eMD := native.NewEvent(eDesc)
	m.AddEvent(eMD)

	return m
}

func logMessage(ic *interop.Context, msg string, fields ...zap.Field) {
	var txHash string
	if ic.Tx != nil {
		txHash = ic.Tx.Hash().StringLE()
	}
	ic.Log.Info(runtime.SystemRuntimeLogMessage, append(fields,
		zap.String("tx", txHash),
		zap.String("script", ic.VM.GetCurrentScriptHash().StringLE()),
		zap.String("msg", msg))...,
	)
}
