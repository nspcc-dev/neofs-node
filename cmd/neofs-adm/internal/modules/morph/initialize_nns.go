package morph

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

const defaultNameServiceDomainPrice = 10_0000_0000
const defaultRegisterSysfee = 10_0000_0000 + defaultNameServiceDomainPrice

func (c *initializeContext) setNNS() error {
	nnsCs, err := c.Client.GetContractStateByID(1)
	if err != nil {
		return err
	}

	ok, err := c.nnsRootRegistered(nnsCs.Hash)
	if err != nil {
		return err
	} else if !ok {
		bw := io.NewBufBinWriter()
		emit.AppCall(bw.BinWriter, nnsCs.Hash, "register", callflag.All,
			"neofs", c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(604800), int64(3600))
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
		if err := c.sendCommitteeTx(bw.Bytes(), -1); err != nil {
			return fmt.Errorf("can't add domain root to NNS: %w", err)
		}
		if err := c.awaitTx(); err != nil {
			return err
		}
	}

	alphaCs := c.getContract(alphabetContract)
	for i, acc := range c.Accounts {
		alphaCs.Hash = state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)

		domain := getAlphabetNNSDomain(i)
		if err := c.nnsRegisterDomain(nnsCs.Hash, alphaCs.Hash, domain); err != nil {
			return err
		}
		c.Command.Printf("NNS: Set %s -> %s\n", domain, alphaCs.Hash.StringLE())
	}

	for _, ctrName := range contractList {
		cs := c.getContract(ctrName)

		domain := ctrName + ".neofs"
		if err := c.nnsRegisterDomain(nnsCs.Hash, cs.Hash, domain); err != nil {
			return err
		}
		c.Command.Printf("NNS: Set %s -> %s\n", domain, cs.Hash.StringLE())
	}

	groupKey := c.ContractWallet.Accounts[0].PrivateKey().PublicKey()
	err = c.updateNNSGroup(nnsCs.Hash, groupKey)
	if err != nil {
		return err
	}
	c.Command.Printf("NNS: Set %s -> %s\n", groupKeyDomain, hex.EncodeToString(groupKey.Bytes()))

	return c.awaitTx()
}

func (c *initializeContext) updateNNSGroup(nnsHash util.Uint160, pub *keys.PublicKey) error {
	bw := io.NewBufBinWriter()
	sysFee, err := c.emitUpdateNNSGroupScript(bw, nnsHash, pub)
	if err != nil {
		return err
	}
	return c.sendCommitteeTx(bw.Bytes(), sysFee)
}

func (c *initializeContext) emitUpdateNNSGroupScript(bw *io.BufBinWriter, nnsHash util.Uint160, pub *keys.PublicKey) (int64, error) {
	isAvail, err := c.Client.NNSIsAvailable(nnsHash, groupKeyDomain)
	if err != nil {
		return 0, err
	}

	if !isAvail {
		currentPub, err := nnsResolveKey(c.Client, nnsHash, groupKeyDomain)
		if err != nil {
			return 0, err
		}

		if pub.Equal(currentPub) {
			return 0, nil
		}
	}

	sysFee := int64(native.GASFactor)
	if isAvail {
		emit.AppCall(bw.BinWriter, nnsHash, "register", callflag.All,
			groupKeyDomain, c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(604800), int64(3600))
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
		sysFee += defaultRegisterSysfee
	}
	emit.AppCall(bw.BinWriter, nnsHash, "addRecord", callflag.All,
		"group.neofs", int64(nns.TXT), hex.EncodeToString(pub.Bytes()))

	return sysFee, bw.Err
}

func getAlphabetNNSDomain(i int) string {
	return alphabetContract + strconv.FormatUint(uint64(i), 10) + ".neofs"
}

func (c *initializeContext) nnsRegisterDomainScript(nnsHash, expectedHash util.Uint160, domain string) ([]byte, error) {
	ok, err := c.Client.NNSIsAvailable(nnsHash, domain)
	if err != nil {
		return nil, err
	}

	bw := io.NewBufBinWriter()
	if ok {
		emit.AppCall(bw.BinWriter, nnsHash, "register", callflag.All,
			domain, c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(604800), int64(3600))
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
	} else {
		s, err := nnsResolveHash(c.Client, nnsHash, domain)
		if err != nil {
			return nil, err
		}
		if s == expectedHash {
			return nil, nil
		}
	}

	emit.AppCall(bw.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, int64(nns.TXT), expectedHash.StringLE())

	if bw.Err != nil {
		panic(bw.Err)
	}
	return bw.Bytes(), nil
}

func (c *initializeContext) nnsRegisterDomain(nnsHash, expectedHash util.Uint160, domain string) error {
	script, err := c.nnsRegisterDomainScript(nnsHash, expectedHash, domain)
	if script == nil {
		return err
	}
	sysFee := int64(defaultRegisterSysfee + native.GASFactor)
	return c.sendCommitteeTx(script, sysFee)
}

func (c *initializeContext) nnsRootRegistered(nnsHash util.Uint160) (bool, error) {
	params := []smartcontract.Parameter{{Type: smartcontract.StringType, Value: "name.neofs"}}
	res, err := c.Client.InvokeFunction(nnsHash, "isAvailable", params, nil)
	if err != nil {
		return false, err
	}
	return res.State == vm.HaltState.String(), nil
}

var errMissingNNSRecord = errors.New("missing NNS record")

// Returns errMissingNNSRecord if invocation fault exception contains "token not found".
func nnsResolveHash(c *client.Client, nnsHash util.Uint160, domain string) (util.Uint160, error) {
	item, err := nnsResolve(c, nnsHash, domain)
	if err != nil {
		return util.Uint160{}, err
	}
	return parseNNSResolveResult(item)
}

func nnsResolve(c *client.Client, nnsHash util.Uint160, domain string) (stackitem.Item, error) {
	result, err := c.InvokeFunction(nnsHash, "resolve", []smartcontract.Parameter{
		{
			Type:  smartcontract.StringType,
			Value: domain,
		},
		{
			Type:  smartcontract.IntegerType,
			Value: int64(nns.TXT),
		},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("`resolve`: %w", err)
	}
	if result.State != vm.HaltState.String() {
		if strings.Contains(result.FaultException, "token not found") {
			return nil, errMissingNNSRecord
		}
		return nil, fmt.Errorf("invocation failed: %s", result.FaultException)
	}
	if len(result.Stack) == 0 {
		return nil, errors.New("result stack is empty")
	}
	return result.Stack[len(result.Stack)-1], nil
}

func nnsResolveKey(c *client.Client, nnsHash util.Uint160, domain string) (*keys.PublicKey, error) {
	item, err := nnsResolve(c, nnsHash, domain)
	if err != nil {
		return nil, err
	}
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) == 0 {
		return nil, errors.New("NNS record is missing")
	}
	bs, err := arr[0].TryBytes()
	if err != nil {
		return nil, errors.New("malformed response")
	}

	return keys.NewPublicKeyFromString(string(bs))
}

// parseNNSResolveResult parses the result of resolving NNS record.
// It works with multiple formats (corresponding to multiple NNS versions).
// If array of hashes is provided, it returns only the first one.
func parseNNSResolveResult(res stackitem.Item) (util.Uint160, error) {
	if arr, ok := res.Value().([]stackitem.Item); ok {
		if len(arr) == 0 {
			return util.Uint160{}, errors.New("NNS record is missing")
		}
		res = arr[0]
	}
	bs, err := res.TryBytes()
	if err != nil {
		return util.Uint160{}, errors.New("malformed response")
	}
	return util.Uint160DecodeStringLE(string(bs))
}
