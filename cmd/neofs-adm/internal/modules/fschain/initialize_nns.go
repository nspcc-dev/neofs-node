package fschain

import (
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
)

const defaultExpirationTime = 10 * 365 * 24 * time.Hour / time.Second

func getAlphabetNNSDomain(i int) string {
	return nns.NameAlphabetPrefix + strconv.FormatUint(uint64(i), 10)
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
