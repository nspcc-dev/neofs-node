package morph

import (
	"errors"
	"fmt"
	"strconv"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
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
const defaultNameServiceSysfee = 4000_0000
const defaultRegisterSysfee = 10_0000_0000 + defaultNameServiceDomainPrice

func (c *initializeContext) setNNS() error {
	nnsCs := c.getContract(nnsContract)

	ok, err := c.nnsRootRegistered(nnsCs.Hash)
	if err != nil {
		return err
	} else if !ok {
		bw := io.NewBufBinWriter()
		emit.AppCall(bw.BinWriter, nnsCs.Hash, "addRoot", callflag.All, "neofs")
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

	return c.awaitTx()
}

func getAlphabetNNSDomain(i int) string {
	return alphabetContract + strconv.FormatUint(uint64(i), 10) + ".neofs"
}

func (c *initializeContext) nnsRegisterDomain(nnsHash, expectedHash util.Uint160, domain string) error {
	ok, err := c.Client.NNSIsAvailable(nnsHash, domain)
	if err != nil {
		return err
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
			return err
		}
		if s != expectedHash {
			return nil
		}
	}

	emit.AppCall(bw.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, int64(nns.TXT), expectedHash.StringLE())

	if bw.Err != nil {
		panic(bw.Err)
	}

	sysFee := int64(defaultRegisterSysfee + native.GASFactor)
	return c.sendCommitteeTx(bw.Bytes(), sysFee)
}

func (c *initializeContext) nnsRootRegistered(nnsHash util.Uint160) (bool, error) {
	params := []smartcontract.Parameter{{Type: smartcontract.StringType, Value: "name.neofs"}}
	res, err := c.Client.InvokeFunction(nnsHash, "isAvailable", params, nil)
	if err != nil {
		return false, err
	}
	return res.State == vm.HaltState.String(), nil
}

func nnsResolveHash(c *client.Client, nnsHash util.Uint160, domain string) (util.Uint160, error) {
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
		return util.Uint160{}, fmt.Errorf("`resolve`: %w", err)
	}
	if result.State != vm.HaltState.String() {
		return util.Uint160{}, fmt.Errorf("invocation failed: %s", result.FaultException)
	}
	if len(result.Stack) == 0 {
		return util.Uint160{}, errors.New("result stack is empty")
	}
	return parseNNSResolveResult(result.Stack[len(result.Stack)-1])
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
