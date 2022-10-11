package blockchain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// directRPCActor provides actor.RPCActor without actual RPC: it uses core
// blockchain components of the Neo Go node directly.
//
// Planned to be natively supported in Neo Go, see
// https://github.com/nspcc-dev/neo-go/issues/2909.
type directRPCActor struct {
	blockchain *core.Blockchain

	netServer *network.Server

	maxGasInvoke int64
}

// newActor provides notary.RPCActor based on the given core.Blockchain,
// network.Server and Neo Go node configuration.
func newActor(b *core.Blockchain, s *network.Server, maxGASInvoke fixedn.Fixed8) notary.RPCActor {
	return &directRPCActor{
		blockchain:   b,
		netServer:    s,
		maxGasInvoke: int64(maxGASInvoke),
	}
}

func (x *directRPCActor) TerminateSession(sessionID uuid.UUID) (bool, error) {
	// TODO: implement
	panic("unexpected TerminateSession call")
}

func (x *directRPCActor) TraverseIterator(sessionID, iteratorID uuid.UUID, maxItemsCount int) ([]stackitem.Item, error) {
	// TODO: implement
	panic("unexpected TerminateIterator call")
}

func (x *directRPCActor) InvokeContractVerify(contract util.Uint160, params []smartcontract.Parameter, signers []transaction.Signer, witnesses ...transaction.Witness) (*result.Invoke, error) {
	// TODO: implement
	panic("unexpected InvokeContractVerify call")
}

func (x *directRPCActor) InvokeFunction(contract util.Uint160, method string, params []smartcontract.Parameter, signers []transaction.Signer) (*result.Invoke, error) {
	// copy-paste from neofs-adm, it'd be nice to share
	var err error
	paramsAny := make([]interface{}, len(params))

	for i := range params {
		paramsAny[i], err = smartcontract.ExpandParameterToEmitable(params[i])
		if err != nil {
			return nil, fmt.Errorf("convert parameter %s: %w", params[i].Type, err)
		}
	}

	w := io.NewBufBinWriter()
	emit.Array(w.BinWriter, paramsAny...)
	emit.AppCallNoArgs(w.BinWriter, contract, method, callflag.All)

	if w.Err != nil {
		return nil, fmt.Errorf("emit contract call: %w", w.Err)
	}

	lastBlock, err := x.blockchain.GetBlock(x.blockchain.CurrentBlockHash())
	if err != nil {
		return nil, err
	}

	script := w.Bytes()

	tx := transaction.New(script, 0)
	tx.Signers = signers
	tx.ValidUntilBlock = x.blockchain.BlockHeight() + 2

	interopCtx, err := x.blockchain.GetTestVM(trigger.Application, tx, &block.Block{
		Header: block.Header{
			Index:     lastBlock.Index + 1,
			Timestamp: lastBlock.Timestamp + 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get blockchain test VM: %w", err)
	}

	interopCtx.VM.GasLimit = 100_0000_0000 // sure?
	interopCtx.VM.LoadScriptWithFlags(script, callflag.All)

	err = interopCtx.VM.Run()

	var res result.Invoke
	res.State = interopCtx.VM.State().String()
	res.GasConsumed = interopCtx.VM.GasConsumed()
	res.Script = script
	res.Stack = interopCtx.VM.Estack().ToArray()
	// how about other fields?

	if err != nil {
		res.FaultException = err.Error()
	}

	return &res, nil
}

func (x *directRPCActor) InvokeScript(script []byte, signers []transaction.Signer) (*result.Invoke, error) {
	lastBlock, err := x.blockchain.GetBlock(x.blockchain.CurrentBlockHash())
	if err != nil {
		return nil, err
	}

	tx := transaction.New(script, 0)
	tx.Signers = signers
	tx.ValidUntilBlock = x.blockchain.BlockHeight() + 2

	ic, err := x.blockchain.GetTestVM(trigger.Application, tx, &block.Block{
		Header: block.Header{
			Index:     lastBlock.Index + 1,
			Timestamp: lastBlock.Timestamp + 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get test VM: %w", err)
	}

	ic.VM.GasLimit = 100_0000_0000
	ic.VM.LoadScriptWithFlags(script, callflag.All)

	var errStr string
	if err := ic.VM.Run(); err != nil {
		errStr = err.Error()
	}

	return &result.Invoke{
		State:          ic.VM.State().String(),
		GasConsumed:    ic.VM.GasConsumed(),
		Script:         script,
		Stack:          ic.VM.Estack().ToArray(),
		FaultException: errStr,
	}, nil
}

func (x *directRPCActor) CalculateNetworkFee(_tx *transaction.Transaction) (int64, error) {
	// we are going to call Hash method, so we must pre-copy transaction
	// as required in method docs
	tx := transaction.Transaction{
		Version:         _tx.Version,
		Nonce:           _tx.Nonce,
		SystemFee:       _tx.SystemFee,
		NetworkFee:      _tx.NetworkFee,
		ValidUntilBlock: _tx.ValidUntilBlock,
		Script:          _tx.Script,
		Attributes:      _tx.Attributes,
		Signers:         _tx.Signers,
		Scripts:         _tx.Scripts,
		Trimmed:         _tx.Trimmed,
	}

	hashablePart, err := tx.EncodeHashableFields()
	if err != nil {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("failed to compute tx size: %s", err))
	}
	size := len(hashablePart) + io.GetVarSize(len(tx.Signers))
	var netFee int64
	for i, signer := range tx.Signers {
		w := tx.Scripts[i]
		if len(w.InvocationScript) == 0 { // No invocation provided, try to infer one.
			var paramz []manifest.Parameter
			if len(w.VerificationScript) == 0 { // Contract-based verification
				cs := x.blockchain.GetContractState(signer.Account)
				if cs == nil {
					return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("signer %d has no verification script and no deployed contract", i))
				}
				md := cs.Manifest.ABI.GetMethod(manifest.MethodVerify, -1)
				if md == nil || md.ReturnType != smartcontract.BoolType {
					return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("signer %d has no verify method in deployed contract", i))
				}
				paramz = md.Parameters // Might as well have none params and it's OK.
			} else { // Regular signature verification.
				if vm.IsSignatureContract(w.VerificationScript) {
					paramz = []manifest.Parameter{{Type: smartcontract.SignatureType}}
				} else if nSigs, _, ok := vm.ParseMultiSigContract(w.VerificationScript); ok {
					paramz = make([]manifest.Parameter, nSigs)
					for j := 0; j < nSigs; j++ {
						paramz[j] = manifest.Parameter{Type: smartcontract.SignatureType}
					}
				}
			}
			inv := io.NewBufBinWriter()
			for _, p := range paramz {
				p.Type.EncodeDefaultValue(inv.BinWriter)
			}
			if inv.Err != nil {
				return 0, neorpc.NewInternalServerError(fmt.Sprintf("failed to create dummy invocation script (signer %d): %s", i, inv.Err.Error()))
			}
			w.InvocationScript = inv.Bytes()
		}
		gasConsumed, _ := x.blockchain.VerifyWitness(signer.Account, &tx, &w, x.maxGasInvoke)
		netFee += gasConsumed
		size += io.GetVarSize(w.VerificationScript) + io.GetVarSize(w.InvocationScript)
	}
	if x.blockchain.P2PSigExtensionsEnabled() {
		attrs := tx.GetAttributes(transaction.NotaryAssistedT)
		if len(attrs) != 0 {
			na := attrs[0].Value.(*transaction.NotaryAssisted)
			netFee += (int64(na.NKeys) + 1) * x.blockchain.GetNotaryServiceFeePerKey()
		}
	}
	_fee := x.blockchain.FeePerByte()
	netFee += int64(size) * _fee
	return netFee, nil
}

// GetBlockCount returns number of the highest block in the underlying core.Blockchain.
func (x *directRPCActor) GetBlockCount() (uint32, error) {
	return x.blockchain.BlockHeight(), nil
}

// GetVersion returns result.Version based on the configuration of underlying
// core.Blockchain and network.Server components.
func (x *directRPCActor) GetVersion() (*result.Version, error) {
	port, err := x.netServer.Port(nil) // any port will suite
	if err != nil {
		return nil, fmt.Errorf("fetch server port: %w", err)
	}

	cfg := x.blockchain.GetConfig()

	var res result.Version
	res.TCPPort = port
	res.Nonce = x.netServer.ID()
	res.UserAgent = x.netServer.UserAgent
	res.Protocol.AddressVersion = address.NEO3Prefix
	res.Protocol.Network = cfg.Magic
	res.Protocol.MillisecondsPerBlock = int(cfg.TimePerBlock / time.Millisecond)
	res.Protocol.MaxTraceableBlocks = cfg.MaxTraceableBlocks
	res.Protocol.MaxValidUntilBlockIncrement = cfg.MaxValidUntilBlockIncrement
	res.Protocol.MaxTransactionsPerBlock = cfg.MaxTransactionsPerBlock
	res.Protocol.MemoryPoolMaxTransactions = cfg.MemPoolSize
	res.Protocol.ValidatorsCount = byte(cfg.GetNumOfCNs(x.blockchain.BlockHeight()))
	res.Protocol.InitialGasDistribution = cfg.InitialGASSupply
	res.Protocol.CommitteeHistory = cfg.CommitteeHistory
	res.Protocol.P2PSigExtensions = cfg.P2PSigExtensions
	res.Protocol.StateRootInHeader = cfg.StateRootInHeader
	res.Protocol.ValidatorsHistory = cfg.ValidatorsHistory

	return &res, nil
}

// SendRawTransaction spreads given transaction to the blockchain network
// using underlying network.Server.
func (x *directRPCActor) SendRawTransaction(tx *transaction.Transaction) (util.Uint256, error) {
	h := tx.Hash()
	return h, x.netServer.RelayTxn(tx)
}

// SubmitP2PNotaryRequest verifies and spreads given notary request to the blockchain
// network using underlying network.Server.
func (x *directRPCActor) SubmitP2PNotaryRequest(req *payload.P2PNotaryRequest) (res util.Uint256, err error) {
	err = x.netServer.RelayP2PNotaryRequest(req)
	if err != nil {
		return res, fmt.Errorf("relay notary request using network server: %w", err)
	}

	return req.FallbackTransaction.Hash(), nil
}
