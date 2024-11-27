package fschain

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
)

const defaultExpirationTime = 10 * 365 * 24 * time.Hour / time.Second

func (c *initializeContext) setNNS() error {
	nnsHash, err := nns.InferHash(c.Client)
	if err != nil {
		return err
	}

	ok, err := c.nnsRootRegistered(nnsHash, "neofs")
	if err != nil {
		return err
	} else if !ok {
		version, err := unwrap.Int64(c.CommitteeAct.Call(nnsHash, "version"))
		if err != nil {
			return err
		}

		bw := io.NewBufBinWriter()
		if version < 18_000 { // 0.18.0
			emit.AppCall(bw.BinWriter, nnsHash, "register", callflag.All,
				"neofs", c.CommitteeAcc.Contract.ScriptHash(),
				"ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
		} else {
			emit.AppCall(bw.BinWriter, nnsHash, "registerTLD", callflag.All,
				"neofs", "ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
		}
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
		if err := c.sendCommitteeTx(bw.Bytes(), true); err != nil {
			return fmt.Errorf("can't add domain root to NNS: %w", err)
		}
		if err := c.awaitTx(); err != nil {
			return err
		}
	}

	alphaCs := c.getContract(alphabetContract)
	for i, acc := range c.Accounts {
		alphaCs.Hash = state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)

		domain := getAlphabetNNSDomain(i) + "." + nns.ContractTLD
		if err := c.nnsRegisterDomain(nnsHash, alphaCs.Hash, domain); err != nil {
			return err
		}
		c.Command.Printf("NNS: Set %s -> %s\n", domain, alphaCs.Hash.StringLE())
	}

	for _, ctrName := range contractList {
		cs := c.getContract(ctrName)

		domain := ctrName + ".neofs"
		if err := c.nnsRegisterDomain(nnsHash, cs.Hash, domain); err != nil {
			return err
		}
		c.Command.Printf("NNS: Set %s -> %s\n", domain, cs.Hash.StringLE())
	}

	return c.awaitTx()
}

func getAlphabetNNSDomain(i int) string {
	return nns.NameAlphabetPrefix + strconv.FormatUint(uint64(i), 10)
}

// wrapRegisterScriptWithPrice wraps a given script with `getPrice`/`setPrice` calls for NNS.
// It is intended to be used for a single transaction, and not as a part of other scripts.
// It is assumed that script already contains static slot initialization code, the first one
// (with index 0) is used to store the price.
func wrapRegisterScriptWithPrice(w *io.BufBinWriter, nnsHash util.Uint160, s []byte) {
	if len(s) == 0 {
		return
	}

	emit.AppCall(w.BinWriter, nnsHash, "getPrice", callflag.All)
	emit.Opcodes(w.BinWriter, opcode.STSFLD0)
	emit.AppCall(w.BinWriter, nnsHash, "setPrice", callflag.All, 1)

	w.WriteBytes(s)

	emit.Opcodes(w.BinWriter, opcode.LDSFLD0, opcode.PUSH1, opcode.PACK)
	emit.AppCallNoArgs(w.BinWriter, nnsHash, "setPrice", callflag.All)

	if w.Err != nil {
		panic(fmt.Errorf("BUG: can't wrap register script: %w", w.Err))
	}
}

func (c *initializeContext) nnsRegisterDomainScript(nnsHash, expectedHash util.Uint160, domain string) ([]byte, bool, error) {
	nnsReader := nns.NewReader(c.ReadOnlyInvoker, nnsHash)
	ok, err := nnsReader.IsAvailable(domain)
	if err != nil {
		return nil, false, err
	}

	if ok {
		bw := io.NewBufBinWriter()
		emit.AppCall(bw.BinWriter, nnsHash, "register", callflag.All,
			domain, c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)

		if bw.Err != nil {
			panic(bw.Err)
		}
		return bw.Bytes(), false, nil
	}

	s, err := nnsResolveHash(nnsReader, domain)
	if err != nil {
		return nil, false, err
	}
	return nil, s == expectedHash, nil
}

func (c *initializeContext) nnsRegisterDomain(nnsHash, expectedHash util.Uint160, domain string) error {
	script, ok, err := c.nnsRegisterDomainScript(nnsHash, expectedHash, domain)
	if ok || err != nil {
		return err
	}

	w := io.NewBufBinWriter()
	emit.Instruction(w.BinWriter, opcode.INITSSLOT, []byte{1})
	wrapRegisterScriptWithPrice(w, nnsHash, script)

	emit.AppCall(w.BinWriter, nnsHash, "deleteRecords", callflag.All, domain, nns.TXT)
	emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, nns.TXT, expectedHash.StringLE())
	emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, nns.TXT, address.Uint160ToString(expectedHash))
	return c.sendCommitteeTx(w.Bytes(), true)
}

func (c *initializeContext) nnsRootRegistered(nnsHash util.Uint160, zone string) (bool, error) {
	res, err := c.CommitteeAct.Call(nnsHash, "isAvailable", "name."+zone)
	if err != nil {
		return false, err
	}

	return res.State == vmstate.Halt.String(), nil
}

func nnsResolveHash(nnsReader *nns.ContractReader, domain string) (util.Uint160, error) {
	itms, err := nnsReader.Resolve(domain, nns.TXT)
	if err != nil {
		return util.Uint160{}, err
	}
	return nns.AddressFromRecords(itms)
}
