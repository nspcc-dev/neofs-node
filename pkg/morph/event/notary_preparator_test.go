package event

import (
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
	alphaKeys      keys.PublicKeys
	wrongAlphaKeys keys.PublicKeys

	dummyInvocationScript      = append([]byte{byte(opcode.PUSHDATA1), 64}, make([]byte, 64)...)
	wrongDummyInvocationScript = append([]byte{byte(opcode.PUSHDATA1), 64, 1}, make([]byte, 63)...)

	scriptHash util.Uint160
)

func init() {
	privat, _ := keys.NewPrivateKey()
	pub := privat.PublicKey()

	alphaKeys = keys.PublicKeys{pub}

	wrongPrivat, _ := keys.NewPrivateKey()
	wrongPub := wrongPrivat.PublicKey()

	wrongAlphaKeys = keys.PublicKeys{wrongPub}

	scriptHash, _ = util.Uint160DecodeStringLE("21fce15191428e9c2f0e8d0329ff6d3dd14882de")
}

type blockCounter struct {
	epoch uint32
	err   error
}

func (b blockCounter) BlockCount() (res uint32, err error) {
	return b.epoch, b.err
}

func TestPrepare_IncorrectScript(t *testing.T) {
	preparator := notaryPreparator(
		PreparatorPrm{
			alphaKeysSource(),
			blockCounter{100, nil},
		},
	)

	t.Run("not contract call", func(t *testing.T) {
		bw := io.NewBufBinWriter()

		emit.Int(bw.BinWriter, 4)
		emit.String(bw.BinWriter, "test")
		emit.Bytes(bw.BinWriter, scriptHash.BytesBE())
		emit.Syscall(bw.BinWriter, interopnames.SystemContractCallNative) // any != interopnames.SystemContractCall

		nr := correctNR(bw.Bytes())

		_, err := preparator.Prepare(nr)

		require.EqualError(t, err, errNotContractCall.Error())
	})

	t.Run("incorrect ", func(t *testing.T) {
		bw := io.NewBufBinWriter()

		emit.Int(bw.BinWriter, -1)
		emit.String(bw.BinWriter, "test")
		emit.Bytes(bw.BinWriter, scriptHash.BytesBE())
		emit.Syscall(bw.BinWriter, interopnames.SystemContractCall)

		nr := correctNR(bw.Bytes())

		_, err := preparator.Prepare(nr)

		require.EqualError(t, err, errIncorrectCallFlag.Error())
	})
}

func TestPrepare_IncorrectNR(t *testing.T) {
	type (
		mTX struct {
			sigs    []transaction.Signer
			scripts []transaction.Witness
			attrs   []transaction.Attribute
		}
		fbTX struct {
			attrs []transaction.Attribute
		}
	)

	setIncorrectFields := func(nr payload.P2PNotaryRequest, m mTX, f fbTX) payload.P2PNotaryRequest {
		if m.sigs != nil {
			nr.MainTransaction.Signers = m.sigs
		}

		if m.scripts != nil {
			nr.MainTransaction.Scripts = m.scripts
		}

		if m.attrs != nil {
			nr.MainTransaction.Attributes = m.attrs
		}

		if f.attrs != nil {
			nr.FallbackTransaction.Attributes = f.attrs
		}

		return nr
	}

	alphaVerificationScript, _ := smartcontract.CreateMultiSigRedeemScript(len(alphaKeys)*2/3+1, alphaKeys)
	wrongAlphaVerificationScript, _ := smartcontract.CreateMultiSigRedeemScript(len(wrongAlphaKeys)*2/3+1, wrongAlphaKeys)

	tests := []struct {
		name   string
		mTX    mTX
		fbTX   fbTX
		expErr error
	}{
		{
			name: "incorrect witness amount",
			mTX: mTX{
				scripts: []transaction.Witness{{}},
			},
			expErr: errUnexpectedWitnessAmount,
		},
		{
			name: "not dummy invocation script",
			mTX: mTX{
				scripts: []transaction.Witness{
					{},
					{
						InvocationScript: wrongDummyInvocationScript,
					},
					{},
				},
			},
			expErr: ErrTXAlreadyHandled,
		},
		{
			name: "incorrect main TX signers amount",
			mTX: mTX{
				sigs: []transaction.Signer{{}},
			},
			expErr: errUnexpectedCosignersAmount,
		},
		{
			name: "incorrect main TX Alphabet signer",
			mTX: mTX{
				sigs: []transaction.Signer{
					{},
					{
						Account: hash.Hash160(wrongAlphaVerificationScript),
					},
					{},
				},
			},
			expErr: errIncorrectAlphabetSigner,
		},
		{
			name: "incorrect main TX attribute amount",
			mTX: mTX{
				attrs: []transaction.Attribute{{}, {}},
			},
			expErr: errIncorrectAttributesAmount,
		},
		{
			name: "incorrect main TX attribute",
			mTX: mTX{
				attrs: []transaction.Attribute{
					{
						Value: &transaction.NotaryAssisted{
							NKeys: uint8(len(alphaKeys) + 1),
						},
					},
				},
			},
			expErr: errIncorrectAttribute,
		},
		{
			name: "incorrect main TX proxy witness",
			mTX: mTX{
				scripts: []transaction.Witness{
					{
						InvocationScript: make([]byte, 1),
					},
					{
						InvocationScript: dummyInvocationScript,
					},
					{},
				},
			},
			expErr: errIncorrectProxyWitnesses,
		},
		{
			name: "incorrect main TX Alphabet witness",
			mTX: mTX{
				scripts: []transaction.Witness{
					{},
					{
						VerificationScript: wrongAlphaVerificationScript,
						InvocationScript:   dummyInvocationScript,
					},
					{},
				},
			},
			expErr: errIncorrectAlphabet,
		},
		{
			name: "incorrect main TX Notary witness",
			mTX: mTX{
				scripts: []transaction.Witness{
					{},
					{
						VerificationScript: alphaVerificationScript,
						InvocationScript:   dummyInvocationScript,
					},
					{
						InvocationScript: wrongDummyInvocationScript,
					},
				},
			},
			expErr: errIncorrectNotaryPlaceholder,
		},
		{
			name: "incorrect fb TX attributes amount",
			fbTX: fbTX{
				attrs: []transaction.Attribute{{}},
			},
			expErr: errIncorrectFBAttributesAmount,
		},
		{
			name: "incorrect fb TX attributes",
			fbTX: fbTX{
				attrs: []transaction.Attribute{{}, {}, {}},
			},
			expErr: errIncorrectFBAttributes,
		},
		{
			name: "expired fb TX",
			fbTX: fbTX{
				[]transaction.Attribute{
					{},
					{
						Type: transaction.NotValidBeforeT,
						Value: &transaction.NotValidBefore{
							Height: 1,
						},
					},
					{},
				},
			},
			expErr: ErrMainTXExpired,
		},
	}

	preparator := notaryPreparator(
		PreparatorPrm{
			alphaKeysSource(),
			blockCounter{100, nil},
		},
	)

	var (
		incorrectNR payload.P2PNotaryRequest
		err         error
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			correctNR := correctNR(nil)
			incorrectNR = setIncorrectFields(*correctNR, test.mTX, test.fbTX)

			_, err = preparator.Prepare(&incorrectNR)

			require.EqualError(t, err, test.expErr.Error())
		})
	}
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
