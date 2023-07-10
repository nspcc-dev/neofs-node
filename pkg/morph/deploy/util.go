package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/interopnames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-contract/common"
	"go.uber.org/zap"
)

func isErrContractAlreadyUpdated(err error) bool {
	return strings.Contains(err.Error(), common.ErrAlreadyUpdated)
}

func isErrTLDNotFound(err error) bool {
	return strings.Contains(err.Error(), "TLD not found")
}

func setGroupInManifest(manif *manifest.Manifest, nefFile nef.File, groupPrivKey *keys.PrivateKey, deployerAcc util.Uint160) {
	contractAddress := state.CreateContractHash(deployerAcc, nefFile.Checksum, manif.Name)
	sig := groupPrivKey.Sign(contractAddress.BytesBE())
	groupPubKey := groupPrivKey.PublicKey()

	ind := -1

	for i := range manif.Groups {
		if manif.Groups[i].PublicKey.Equal(groupPubKey) {
			ind = i
			break
		}
	}

	if ind >= 0 {
		manif.Groups[ind].Signature = sig
		return
	}

	manif.Groups = append(manif.Groups, manifest.Group{
		PublicKey: groupPubKey,
		Signature: sig,
	})
}

// blockchainMonitor is a thin utility around Blockchain providing state
// monitoring.
type blockchainMonitor struct {
	logger *zap.Logger

	blockchain Blockchain

	blockInterval time.Duration

	subID  string
	height atomic.Uint32
}

// newBlockchainMonitor constructs and runs monitor for the given Blockchain.
// Resulting blockchainMonitor must be stopped when no longer needed.
func newBlockchainMonitor(l *zap.Logger, b Blockchain, chNewBlock chan<- struct{}) (*blockchainMonitor, error) {
	ver, err := b.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("request Neo protocol configuration: %w", err)
	}

	initialBlock, err := b.GetBlockCount()
	if err != nil {
		return nil, fmt.Errorf("get current blockchain height: %w", err)
	}

	blockCh := make(chan *block.Block)

	newBlockSubID, err := b.ReceiveBlocks(nil, blockCh)
	if err != nil {
		return nil, fmt.Errorf("subscribe to new blocks of the chain: %w", err)
	}

	res := &blockchainMonitor{
		logger:        l,
		blockchain:    b,
		blockInterval: time.Duration(ver.Protocol.MillisecondsPerBlock) * time.Millisecond,
		subID:         newBlockSubID,
	}

	res.height.Store(initialBlock)

	go func() {
		l.Info("listening to new blocks...")
		for {
			b, ok := <-blockCh
			if !ok {
				close(chNewBlock)
				l.Info("listening to new blocks stopped")
				return
			}

			res.height.Store(b.Index)

			select {
			case chNewBlock <- struct{}{}:
			default:
			}

			l.Info("new block arrived", zap.Uint32("height", b.Index))
		}
	}()

	return res, nil
}

// currentHeight returns current blockchain height.
func (x *blockchainMonitor) currentHeight() uint32 {
	return x.height.Load()
}

// waitForNextBlock blocks until blockchainMonitor encounters new block on the
// chain or provided context is done.
func (x *blockchainMonitor) waitForNextBlock(ctx context.Context) {
	initialBlock := x.currentHeight()

	ticker := time.NewTicker(x.blockInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if x.height.Load() > initialBlock {
				return
			}
		}
	}
}

// stop stops running blockchainMonitor. Stopped blockchainMonitor must not be
// used anymore.
func (x *blockchainMonitor) stop() {
	err := x.blockchain.Unsubscribe(x.subID)
	if err != nil {
		x.logger.Warn("failed to cancel subscription to new blocks", zap.Error(err))
	}
}

// readNNSOnChainState reads state of the NeoFS NNS contract in the given
// Blockchain. Returns both nil if contract is missing.
func readNNSOnChainState(b Blockchain) (*state.Contract, error) {
	// NNS must always have ID=1 in the NeoFS Sidechain
	const nnsContractID = 1
	res, err := b.GetContractStateByID(nnsContractID)
	if err != nil {
		if errors.Is(err, neorpc.ErrUnknownContract) {
			return nil, nil
		}
		return nil, fmt.Errorf("read contract state by ID=%d: %w", nnsContractID, err)
	}
	return res, nil
}

// contractVersion describes versioning of NeoFS smart contracts.
type contractVersion struct{ major, minor, patch uint64 }

// space sizes for major and minor versions of the NeoFS contracts.
const majorSpace, minorSpace = 1e6, 1e3

// equals checks if contractVersion equals to the specified SemVer version.
//
//nolint:unused
func (x contractVersion) equals(major, minor, patch uint64) bool {
	return x.major == major && x.minor == minor && x.patch == patch
}

// returns contractVersion as single integer.
func (x contractVersion) toUint64() uint64 {
	return x.major*majorSpace + x.minor*minorSpace + x.patch
}

// cmp compares x and y and returns:
//
//	-1 if x <  y
//	 0 if x == y
//	+1 if x >  y
func (x contractVersion) cmp(y contractVersion) int {
	xN := x.toUint64()
	yN := y.toUint64()
	if xN < yN {
		return -1
	} else if xN == yN {
		return 0
	}
	return 1
}

func (x contractVersion) String() string {
	const sep = "."
	return fmt.Sprintf("%d%s%d%s%d", x.major, sep, x.minor, sep, x.patch)
}

// parses contractVersion from the invocation result of methodVersion method.
func parseContractVersionFromInvocationResult(res *result.Invoke) (contractVersion, error) {
	bigVersionOnChain, err := unwrap.BigInt(res, nil)
	if err != nil {
		return contractVersion{}, fmt.Errorf("unwrap big integer from '%s' method return: %w", methodVersion, err)
	} else if !bigVersionOnChain.IsUint64() {
		return contractVersion{}, fmt.Errorf("invalid/unsupported format of the '%s' method return: expected uint64, got %v", methodVersion, bigVersionOnChain)
	}

	n := bigVersionOnChain.Uint64()

	mjr := n / majorSpace

	return contractVersion{
		major: mjr,
		minor: (n - mjr*majorSpace) / minorSpace,
		patch: n % minorSpace,
	}, nil
}

// readContractOnChainVersion returns current version of the smart contract
// presented in given Blockchain with specified address.
func readContractOnChainVersion(b Blockchain, onChainAddress util.Uint160) (contractVersion, error) {
	res, err := invoker.New(b, nil).Call(onChainAddress, methodVersion)
	if err != nil {
		return contractVersion{}, fmt.Errorf("call '%s' contract method: %w", methodVersion, err)
	}

	return parseContractVersionFromInvocationResult(res)
}

// readContractLocalVersion returns version of the local smart contract
// represented by its compiled artifacts. Deployment is tested using provided
// invoker on behalf of the committee.
func readContractLocalVersion(rpc invoker.RPCInvoke, committee keys.PublicKeys, localNEF nef.File, localManifest manifest.Manifest, deployArgs ...interface{}) (contractVersion, error) {
	multiSigScript, err := smartcontract.CreateMultiSigRedeemScript(smartcontract.GetMajorityHonestNodeCount(len(committee)), committee)
	if err != nil {
		return contractVersion{}, fmt.Errorf("create committee multi-signature verification script: %w", err)
	}

	jManifest, err := json.Marshal(localManifest)
	if err != nil {
		return contractVersion{}, fmt.Errorf("encode manifest into JSON: %w", err)
	}

	bNEF, err := localNEF.Bytes()
	if err != nil {
		return contractVersion{}, fmt.Errorf("encode NEF into binary: %w", err)
	}

	var deployData interface{}
	if len(deployArgs) > 0 {
		deployData = deployArgs
	}

	script := io.NewBufBinWriter()
	emit.Opcodes(script.BinWriter, opcode.NEWARRAY0)
	emit.Int(script.BinWriter, int64(callflag.All))
	emit.String(script.BinWriter, methodVersion)
	emit.AppCall(script.BinWriter, management.Hash, "deploy", callflag.All, bNEF, jManifest, deployData)
	emit.Opcodes(script.BinWriter, opcode.PUSH2, opcode.PICKITEM)
	emit.Syscall(script.BinWriter, interopnames.SystemContractCall)

	res, err := invoker.New(rpc, []transaction.Signer{
		{
			Account: util.Uint160{}, // zero hash to avoid 'contract already exists' case
			Scopes:  transaction.None,
		},
		{
			Account: hash.Hash160(multiSigScript),
			Scopes:  transaction.Global,
		},
	}).Run(script.Bytes())
	if err != nil {
		return contractVersion{}, fmt.Errorf("run test script deploying contract and calling its '%s' method: %w", methodVersion, err)
	}

	return parseContractVersionFromInvocationResult(res)
}

type transactionGroupWaiter interface {
	WaitAny(ctx context.Context, vub uint32, hashes ...util.Uint256) (*state.AppExecResult, error)
}

type transactionGroupMonitor struct {
	waiter  transactionGroupWaiter
	pending atomic.Bool
}

func newTransactionGroupMonitor(w transactionGroupWaiter) *transactionGroupMonitor {
	return &transactionGroupMonitor{
		waiter: w,
	}
}

func (x *transactionGroupMonitor) reset() {
	x.pending.Store(false)
}

func (x *transactionGroupMonitor) isPending() bool {
	return x.pending.Load()
}

func (x *transactionGroupMonitor) trackPendingTransactionsAsync(ctx context.Context, vub uint32, txs ...util.Uint256) {
	if len(txs) == 0 {
		panic("missing transactions")
	}

	x.pending.Store(true)

	waitCtx, cancel := context.WithCancel(ctx)

	go func() {
		_, _ = x.waiter.WaitAny(waitCtx, vub, txs...)
		x.reset()
		cancel()
	}()
}

var errInvalidContractDomainRecord = errors.New("invalid contract domain record")

// readContractOnChainStateByDomainName reads address state of contract deployed
// in the given Blockchain and recorded in the NNS with the specified domain
// name. Returns errMissingDomain if domain doesn't exist. Returns
// errMissingDomainRecord if domain has no records. Returns
// errInvalidContractDomainRecord if domain record has invalid/unsupported
// format. Returns [neorpc.ErrUnknownContract] if contract is recorded in the NNS but
// missing in the Blockchain.
func readContractOnChainStateByDomainName(b Blockchain, nnsContract util.Uint160, domainName string) (*state.Contract, error) {
	rec, err := lookupNNSDomainRecord(invoker.New(b, nil), nnsContract, domainName)
	if err != nil {
		return nil, err
	}

	// historically two formats may occur
	addr, err := util.Uint160DecodeStringLE(rec)
	if err != nil {
		addr, err = address.StringToUint160(rec)
		if err != nil {
			return nil, fmt.Errorf("%w: domain record '%s' neither NEO address nor little-endian hex-encoded script hash", errInvalidContractDomainRecord, rec)
		}
	}

	res, err := b.GetContractStateByHash(addr)
	if err != nil {
		return nil, fmt.Errorf("get contract by address=%s: %w", addr, err)
	}

	return res, nil
}
