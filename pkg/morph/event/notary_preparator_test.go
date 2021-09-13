package event

import (
	"encoding/binary"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm"

	"github.com/nspcc-dev/neo-go/pkg/core/interop/interopnames"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/stretchr/testify/require"
)

var (
	alphaKeys keys.PublicKeys

	dummyInvocationScript = append([]byte{byte(opcode.PUSHDATA1), 64}, make([]byte, 64)...)
	contractSysCall       = make([]byte, 4)

	scriptHash util.Uint160
)

func init() {
	privat, _ := keys.NewPrivateKey()
	pub := privat.PublicKey()

	alphaKeys = keys.PublicKeys{pub}

	binary.LittleEndian.PutUint32(contractSysCall, interopnames.ToID([]byte(interopnames.SystemContractCall)))

	scriptHash, _ = util.Uint160DecodeStringLE("21fce15191428e9c2f0e8d0329ff6d3dd14882de")
}

type blockCounter struct {
	epoch uint32
	err   error
}

func (b blockCounter) BlockCount() (res uint32, err error) {
	return b.epoch, b.err
}

func TestPrepare_CorrectNR(t *testing.T) {
	tests := []struct {
		hash   util.Uint160
		method string
		args   []interface{}
	}{
		{
			scriptHash,
			"test1",
			nil,
		},
		{
			scriptHash,
			"test2",
			[]interface{}{
				int64(4),
				"test",
				[]interface{}{
					int64(4),
					false,
					true,
				},
			},
		},
	}

	preparator := notaryPreparator(
		PreparatorPrm{
			alphaKeysSource(),
			blockCounter{100, nil},
		},
	)

	for _, test := range tests {
		nr := correctNR(script(test.hash, test.method, test.args...))

		event, err := preparator.Prepare(nr)

		require.NoError(t, err)
		require.Equal(t, test.method, event.Type().String())
		require.Equal(t, test.hash.StringLE(), event.ScriptHash().StringLE())

		// check args parsing
		bw := io.NewBufBinWriter()
		emit.Array(bw.BinWriter, test.args...)

		ctx := vm.NewContext(bw.Bytes())

		opCode, param, err := ctx.Next()
		require.NoError(t, err)

		for _, opGot := range event.Params() {
			require.Equal(t, opCode, opGot.code)
			require.Equal(t, param, opGot.param)

			opCode, param, err = ctx.Next()
			require.NoError(t, err)
		}

		_, _, err = ctx.Next() //  PACK opcode
		require.NoError(t, err)
		_, _, err = ctx.Next() //  packing len opcode
		require.NoError(t, err)

		opCode, _, err = ctx.Next()
		require.NoError(t, err)
		require.Equal(t, opcode.RET, opCode)
	}
}

func alphaKeysSource() client.AlphabetKeys {
	return func() (keys.PublicKeys, error) {
		return alphaKeys, nil
	}
}

func script(hash util.Uint160, method string, args ...interface{}) []byte {
	bw := io.NewBufBinWriter()

	if len(args) > 0 {
		emit.AppCall(bw.BinWriter, hash, method, callflag.All, args)
	} else {
		emit.AppCallNoArgs(bw.BinWriter, hash, method, callflag.All)
	}

	return bw.Bytes()
}

func correctNR(script []byte) *payload.P2PNotaryRequest {
	alphaVerificationScript, _ := smartcontract.CreateMultiSigRedeemScript(len(alphaKeys)*2/3+1, alphaKeys)

	return &payload.P2PNotaryRequest{
		MainTransaction: &transaction.Transaction{
			Signers: []transaction.Signer{
				{},
				{
					Account: hash.Hash160(alphaVerificationScript),
				},
				{},
			},
			Scripts: []transaction.Witness{
				{},
				{
					InvocationScript:   dummyInvocationScript,
					VerificationScript: alphaVerificationScript,
				},
				{
					InvocationScript: dummyInvocationScript,
				},
			},
			Attributes: []transaction.Attribute{
				{
					Value: &transaction.NotaryAssisted{
						NKeys: uint8(len(alphaKeys)),
					},
				},
			},
			Script: script,
		},
		FallbackTransaction: &transaction.Transaction{
			Attributes: []transaction.Attribute{
				{},
				{
					Type: transaction.NotValidBeforeT,
					Value: &transaction.NotValidBefore{
						Height: 1000,
					},
				},
				{},
			},
		},
	}
}
