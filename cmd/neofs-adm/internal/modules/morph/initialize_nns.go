package morph

import (
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
)

const defaultNameServiceDomainPrice = 10_0000_0000
const defaultNameServiceSysfee = 4000_0000
const defaultRegisterSysfee = 10_0000_0000 + defaultNameServiceDomainPrice

func (c *initializeContext) setNNS() error {
	cs, err := c.readContract(nnsContract)
	if err != nil {
		return err
	}

	h := state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
	ok, err := c.nnsRootRegistered(h)
	if err != nil {
		return err
	} else if !ok {
		bw := io.NewBufBinWriter()
		emit.AppCall(bw.BinWriter, h, "addRoot", callflag.All, "neofs")
		if err := c.sendCommitteeTx(bw.Bytes(), -1); err != nil {
			return fmt.Errorf("can't add domain root to NNS: %w", err)
		}
		if err := c.awaitTx(); err != nil {
			return err
		}
	}

	alphaCs, err := c.readContract(alphabetContract)
	if err != nil {
		return fmt.Errorf("can't read alphabet contract: %w", err)
	}
	for i, acc := range c.Accounts {
		alphaCs.Hash = state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)

		domain := getAlphabetNNSDomain(i)
		if err := c.nnsRegisterDomain(h, alphaCs.Hash, domain); err != nil {
			return err
		}
		c.Command.Printf("NNS: Set %s -> %s\n", domain, alphaCs.Hash.StringLE())
	}

	for _, ctrName := range contractList {
		cs, err := c.readContract(ctrName)
		if err != nil {
			return err
		}
		cs.Hash = state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)

		domain := ctrName + ".neofs"
		if err := c.nnsRegisterDomain(h, cs.Hash, domain); err != nil {
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
			domain, c.CommitteeAcc.Contract.ScriptHash())
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
	} else {
		s, err := c.Client.NNSResolve(nnsHash, domain, nns.TXT)
		if err != nil {
			return err
		}
		if s == expectedHash.StringLE() {
			return nil
		}
	}

	emit.AppCall(bw.BinWriter, nnsHash, "setRecord", callflag.All,
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
	s, err := c.NNSResolve(nnsHash, domain, nns.TXT)
	if err != nil {
		return util.Uint160{}, err
	}

	return util.Uint160DecodeStringLE(s)
}
