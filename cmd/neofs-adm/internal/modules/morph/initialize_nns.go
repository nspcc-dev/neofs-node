package morph

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nns"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const defaultExpirationTime = 10 * 365 * 24 * time.Hour / time.Second

func (c *initializeContext) setNNS() error {
	nnsCs, err := c.Client.GetContractStateByID(1)
	if err != nil {
		return err
	}

	ok, err := c.nnsRootRegistered(nnsCs.Hash, "neofs")
	if err != nil {
		return err
	} else if !ok {
		bw := io.NewBufBinWriter()
		emit.AppCall(bw.BinWriter, nnsCs.Hash, "register", callflag.All,
			"neofs", c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
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

	groupKey := c.ContractWallet.Accounts[0].PublicKey()
	err = c.updateNNSGroup(nnsCs.Hash, groupKey)
	if err != nil {
		return err
	}
	c.Command.Printf("NNS: Set %s -> %s\n", morphClient.NNSGroupKeyName, hex.EncodeToString(groupKey.Bytes()))

	return c.awaitTx()
}

func (c *initializeContext) updateNNSGroup(nnsHash util.Uint160, pub *keys.PublicKey) error {
	bw := io.NewBufBinWriter()
	needUpdate, needRegister, err := c.emitUpdateNNSGroupScript(bw, nnsHash, pub)
	if !needUpdate || err != nil {
		return err
	}

	script := bw.Bytes()
	if needRegister {
		w := io.NewBufBinWriter()
		emit.Instruction(w.BinWriter, opcode.INITSSLOT, []byte{1})
		wrapRegisterScriptWithPrice(w, nnsHash, script)
		script = w.Bytes()
	}

	return c.sendCommitteeTx(script, true)
}

// emitUpdateNNSGroupScript emits script for updating group key stored in NNS.
// First return value is true iff the key is already there and nothing should be done.
// Second return value is true iff a domain registration code was emitted.
func (c *initializeContext) emitUpdateNNSGroupScript(bw *io.BufBinWriter, nnsHash util.Uint160, pub *keys.PublicKey) (bool, bool, error) {
	isAvail, err := nnsIsAvailable(c.Client, nnsHash, morphClient.NNSGroupKeyName)
	if err != nil {
		return false, false, err
	}

	if !isAvail {
		currentPub, err := nnsResolveKey(c.ReadOnlyInvoker, nnsHash, morphClient.NNSGroupKeyName)
		if err != nil {
			return false, false, err
		}

		if pub.Equal(currentPub) {
			return true, false, nil
		}
	}

	if isAvail {
		emit.AppCall(bw.BinWriter, nnsHash, "register", callflag.All,
			morphClient.NNSGroupKeyName, c.CommitteeAcc.Contract.ScriptHash(),
			"ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
		emit.Opcodes(bw.BinWriter, opcode.ASSERT)
	}

	emit.AppCall(bw.BinWriter, nnsHash, "deleteRecords", callflag.All, "group.neofs", int64(nns.TXT))
	emit.AppCall(bw.BinWriter, nnsHash, "addRecord", callflag.All,
		"group.neofs", int64(nns.TXT), hex.EncodeToString(pub.Bytes()))

	return false, isAvail, nil
}

func getAlphabetNNSDomain(i int) string {
	return alphabetContract + strconv.FormatUint(uint64(i), 10) + ".neofs"
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
	ok, err := nnsIsAvailable(c.Client, nnsHash, domain)
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

	s, err := nnsResolveHash(c.ReadOnlyInvoker, nnsHash, domain)
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

	emit.AppCall(w.BinWriter, nnsHash, "deleteRecords", callflag.All, domain, int64(nns.TXT))
	emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, int64(nns.TXT), expectedHash.StringLE())
	emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
		domain, int64(nns.TXT), address.Uint160ToString(expectedHash))
	return c.sendCommitteeTx(w.Bytes(), true)
}

func (c *initializeContext) nnsRootRegistered(nnsHash util.Uint160, zone string) (bool, error) {
	res, err := c.CommitteeAct.Call(nnsHash, "isAvailable", "name."+zone)
	if err != nil {
		return false, err
	}

	return res.State == vmstate.Halt.String(), nil
}

var errMissingNNSRecord = errors.New("missing NNS record")

// Returns errMissingNNSRecord if invocation fault exception contains "token not found".
func nnsResolveHash(inv *invoker.Invoker, nnsHash util.Uint160, domain string) (util.Uint160, error) {
	item, err := nnsResolve(inv, nnsHash, domain)
	if err != nil {
		return util.Uint160{}, err
	}
	return parseNNSResolveResult(item)
}

func nnsResolve(inv *invoker.Invoker, nnsHash util.Uint160, domain string) (stackitem.Item, error) {
	return unwrap.Item(inv.Call(nnsHash, "resolve", domain, int64(nns.TXT)))
}

func nnsResolveKey(inv *invoker.Invoker, nnsHash util.Uint160, domain string) (*keys.PublicKey, error) {
	item, err := nnsResolve(inv, nnsHash, domain)
	if err != nil {
		return nil, err
	}
	_, ok := item.Value().(stackitem.Null)
	if ok {
		return nil, errors.New("NNS record is missing")
	}
	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) < 1 {
		return nil, errors.New("malformed response (not an array)")
	}
	bs, err := arr[0].TryBytes()
	if err != nil {
		return nil, errors.New("malformed response (no bytes)")
	}

	return keys.NewPublicKeyFromString(string(bs))
}

// parseNNSResolveResult parses the result of resolving NNS record.
// It works with multiple formats (corresponding to multiple NNS versions).
// If array of hashes is provided, it returns only the first one.
func parseNNSResolveResult(res stackitem.Item) (util.Uint160, error) {
	arr, ok := res.Value().([]stackitem.Item)
	if !ok {
		arr = []stackitem.Item{res}
	}
	if _, ok := res.Value().(stackitem.Null); ok || len(arr) == 0 {
		return util.Uint160{}, errors.New("NNS record is missing")
	}
	for i := range arr {
		bs, err := arr[i].TryBytes()
		if err != nil {
			continue
		}

		// We support several formats for hash encoding, this logic should be maintained in sync
		// with nnsResolve from pkg/morph/client/nns.go
		h, err := util.Uint160DecodeStringLE(string(bs))
		if err == nil {
			return h, nil
		}

		h, err = address.StringToUint160(string(bs))
		if err == nil {
			return h, nil
		}
	}
	return util.Uint160{}, errors.New("no valid hashes are found")
}

func nnsIsAvailable(c Client, nnsHash util.Uint160, name string) (bool, error) {
	switch c.(type) {
	case *rpcclient.Client:
		invkr := invoker.New(c, nil)
		reader := nns.NewReader(invkr, nnsHash)
		return reader.IsAvailable(name)
	default:
		b, err := unwrap.Bool(invokeFunction(c, nnsHash, "isAvailable", []interface{}{name}, nil))
		if err != nil {
			return false, fmt.Errorf("`isAvailable`: invalid response: %w", err)
		}

		return b, nil
	}
}
