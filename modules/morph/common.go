package morph

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

// SmartContracts maps smart contract name to contract client.
type SmartContracts map[string]implementations.StaticContractClient

// EventHandlers maps notification event name to handler information.
type EventHandlers map[string]event.HandlerInfo

type morphContractsParams struct {
	dig.In

	Viper *viper.Viper

	GoClient *goclient.Client

	Listener event.Listener
}

type contractParams struct {
	dig.In

	Viper *viper.Viper

	Logger *zap.Logger

	MorphContracts SmartContracts

	NodeInfo bootstrap.NodeInfo
}

func newMorphContracts(p morphContractsParams) (SmartContracts, EventHandlers, error) {
	mContracts := make(map[string]implementations.StaticContractClient, len(ContractNames))
	mHandlers := make(map[string]event.HandlerInfo)

	for _, contractName := range ContractNames {
		scHash, err := util.Uint160DecodeStringLE(
			p.Viper.GetString(
				ScriptHashOptPath(contractName),
			),
		)
		if err != nil {
			return nil, nil, err
		}

		fee := util.Fixed8FromInt64(
			p.Viper.GetInt64(
				InvocationFeeOptPath(contractName),
			),
		)

		mContracts[contractName], err = implementations.NewStaticContractClient(p.GoClient, scHash, fee)
		if err != nil {
			return nil, nil, err
		}

		// set event parsers
		parserInfo := event.ParserInfo{}
		parserInfo.SetScriptHash(scHash)

		handlerInfo := event.HandlerInfo{}
		handlerInfo.SetScriptHash(scHash)

		for _, item := range mParsers[contractName] {
			parserInfo.SetParser(item.parser)

			optPath := ContractEventOptPath(contractName, item.typ)

			typEvent := event.TypeFromString(
				p.Viper.GetString(optPath),
			)

			parserInfo.SetType(typEvent)
			handlerInfo.SetType(typEvent)

			p.Listener.SetParser(parserInfo)

			mHandlers[optPath] = handlerInfo
		}
	}

	return mContracts, mHandlers, nil
}

const prefix = "morph"

const (
	endpointOpt = "endpoint"

	dialTimeoutOpt = "dial_timeout"

	magicNumberOpt = "magic_number"

	scriptHashOpt = "script_hash"

	invocationFeeOpt = "invocation_fee"
)

// ContractNames is a list of smart contract names.
var ContractNames = []string{
	containerContractName,
	reputationContractName,
	NetmapContractName,
	BalanceContractName,
}

// EndpointOptPath returns the config path to goclient endpoint.
func EndpointOptPath() string {
	return optPath(prefix, endpointOpt)
}

// MagicNumberOptPath returns the config path to goclient magic number.
func MagicNumberOptPath() string {
	return optPath(prefix, magicNumberOpt)
}

// DialTimeoutOptPath returns the config path to goclient dial timeout.
func DialTimeoutOptPath() string {
	return optPath(prefix, dialTimeoutOpt)
}

// ScriptHashOptPath calculates the config path to script hash config of particular contract.
func ScriptHashOptPath(name string) string {
	return optPath(prefix, name, scriptHashOpt)
}

// InvocationFeeOptPath calculates the config path to invocation fee config of particular contract.
func InvocationFeeOptPath(name string) string {
	return optPath(prefix, name, invocationFeeOpt)
}
