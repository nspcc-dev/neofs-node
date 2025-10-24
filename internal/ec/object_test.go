package ec_test

import (
	"crypto/sha256"
	"math/rand/v2"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

func TestGetPartInfo(t *testing.T) {
	var obj object.Object
	otherAttr := object.NewAttribute("any_attribute", "val")

	obj.SetAttributes(otherAttr)

	t.Run("missing", func(t *testing.T) {
		pi, err := iec.GetPartInfo(obj)
		require.NoError(t, err)
		require.EqualValues(t, -1, pi.RuleIndex)
	})

	t.Run("failure", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			attrs     map[string]string
			assertErr func(t *testing.T, err error)
		}{
			{name: "non-int rule index",
				attrs: map[string]string{"__NEOFS__EC_RULE_IDX": "not_an_int", "__NEOFS__EC_PART_IDX": "456"},
				assertErr: func(t *testing.T, err error) {
					require.ErrorContains(t, err, "invalid index attribute __NEOFS__EC_RULE_IDX: ")
					require.ErrorContains(t, err, "invalid syntax")
				},
			},
			{name: "negative rule index",
				attrs: map[string]string{"__NEOFS__EC_RULE_IDX": "-123", "__NEOFS__EC_PART_IDX": "456"},
				assertErr: func(t *testing.T, err error) {
					require.EqualError(t, err, "invalid index attribute __NEOFS__EC_RULE_IDX: negative value -123")
				},
			},
			{name: "non-int part index",
				attrs: map[string]string{"__NEOFS__EC_RULE_IDX": "123", "__NEOFS__EC_PART_IDX": "not_an_int"},
				assertErr: func(t *testing.T, err error) {
					require.ErrorContains(t, err, "invalid index attribute __NEOFS__EC_PART_IDX: ")
					require.ErrorContains(t, err, "invalid syntax")
				},
			},
			{name: "negative part index",
				attrs: map[string]string{"__NEOFS__EC_RULE_IDX": "123", "__NEOFS__EC_PART_IDX": "-456"},
				assertErr: func(t *testing.T, err error) {
					require.EqualError(t, err, "invalid index attribute __NEOFS__EC_PART_IDX: negative value -456")
				},
			},
			{name: "rule index without part index",
				attrs: map[string]string{"__NEOFS__EC_RULE_IDX": "123"},
				assertErr: func(t *testing.T, err error) {
					require.EqualError(t, err, "__NEOFS__EC_RULE_IDX attribute is set while __NEOFS__EC_PART_IDX is not")
				},
			},
			{name: "part index without rule index",
				attrs: map[string]string{"__NEOFS__EC_PART_IDX": "456"},
				assertErr: func(t *testing.T, err error) {
					require.EqualError(t, err, "__NEOFS__EC_PART_IDX attribute is set while __NEOFS__EC_RULE_IDX is not")
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				attrs := make([]object.Attribute, 0, len(tc.attrs)/2)
				for k, v := range tc.attrs {
					attrs = append(attrs, object.NewAttribute(k, v))
				}

				obj.SetAttributes(append([]object.Attribute{otherAttr}, attrs...)...)

				_, err := iec.GetPartInfo(obj)
				tc.assertErr(t, err)
			})
		}
	})

	obj.SetAttributes(
		otherAttr,
		object.NewAttribute("__NEOFS__EC_RULE_IDX", "123"),
		object.NewAttribute("__NEOFS__EC_PART_IDX", "456"),
	)

	pi, err := iec.GetPartInfo(obj)
	require.NoError(t, err)
	require.Equal(t, iec.PartInfo{RuleIndex: 123, Index: 456}, pi)
}

func TestFormObjectForECPart(t *testing.T) {
	ver := version.Current()
	st := sessiontest.Object()
	signer := neofscryptotest.Signer()

	var parent object.Object
	parent.SetVersion(&ver)
	parent.SetContainerID(cidtest.ID())
	parent.SetOwner(usertest.ID())
	parent.SetCreationEpoch(rand.Uint64())
	parent.SetType(object.Type(rand.Int32()))
	parent.SetSessionToken(&st)
	require.NoError(t, parent.SetVerificationFields(signer))

	partInfo := iec.PartInfo{RuleIndex: 123, Index: 456}
	part := testutil.RandByteSlice(32)

	t.Run("signer failure", func(t *testing.T) {
		signer := neofscryptotest.FailSigner(signer)
		_, sigErr := signer.Sign(nil)
		require.Error(t, sigErr)

		_, err := iec.FormObjectForECPart(signer, parent, part, partInfo)
		require.ErrorContains(t, err, "set verification fields: could not set signature:")
		require.ErrorContains(t, err, sigErr.Error())
	})

	obj, err := iec.FormObjectForECPart(signer, parent, part, partInfo)
	require.NoError(t, err)

	require.NoError(t, obj.VerifyID())
	require.True(t, obj.VerifySignature())

	require.True(t, obj.HasParent())
	require.NotNil(t, obj.Parent())
	require.Equal(t, parent, *obj.Parent())

	require.Equal(t, part, obj.Payload())
	require.EqualValues(t, len(part), obj.PayloadSize())

	pcs, ok := obj.PayloadChecksum()
	require.True(t, ok)
	require.Equal(t, checksum.NewSHA256(sha256.Sum256(part)), pcs)

	require.Equal(t, parent.Version(), obj.Version())
	require.Equal(t, parent.GetContainerID(), obj.GetContainerID())
	require.Equal(t, parent.Owner(), obj.Owner())
	require.Equal(t, parent.CreationEpoch(), obj.CreationEpoch())
	require.Equal(t, object.TypeRegular, obj.Type())
	require.Zero(t, obj.SessionToken())

	_, ok = obj.PayloadHomomorphicHash()
	require.False(t, ok)

	require.Len(t, obj.Attributes(), 2)

	pi, err := iec.GetPartInfo(obj)
	require.NoError(t, err)
	require.Equal(t, partInfo, pi)

	t.Run("with homomorphic hash", func(t *testing.T) {
		anyHash := checksum.NewTillichZemor([tz.Size]byte{1, 2, 3})
		parent.SetPayloadHomomorphicHash(anyHash)

		obj, err := iec.FormObjectForECPart(signer, parent, part, partInfo)
		require.NoError(t, err)

		phh, ok := obj.PayloadHomomorphicHash()
		require.True(t, ok)
		require.Equal(t, checksum.NewTillichZemor(tz.Sum(part)), phh)
	})
}

func TestDecodePartInfoFromAttributes(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		pi, err := iec.DecodePartInfoFromAttributes("", "")
		require.NoError(t, err)
		require.EqualValues(t, -1, pi.RuleIndex)
	})

	t.Run("failure", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			ruleIdx   string
			partIdx   string
			assertErr func(t *testing.T, err error)
		}{
			{name: "non-int rule index", ruleIdx: "not_an_int", partIdx: "34", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, `decode rule index: strconv.ParseUint: parsing "not_an_int": invalid syntax`)
			}},
			{name: "negative rule index", ruleIdx: "-12", partIdx: "34", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, `decode rule index: strconv.ParseUint: parsing "-12": invalid syntax`)
			}},
			{name: "rule index overflow", ruleIdx: "256", partIdx: "34", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, "rule index out of range")
			}},
			{name: "non-int part index", ruleIdx: "12", partIdx: "not_an_int", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, `decode part index: strconv.ParseUint: parsing "not_an_int": invalid syntax`)
			}},
			{name: "negative part index", ruleIdx: "12", partIdx: "-34", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, `decode part index: strconv.ParseUint: parsing "-34": invalid syntax`)
			}},
			{name: "part index overflow", ruleIdx: "12", partIdx: "256", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, "part index out of range")
			}},
			{name: "rule index without part index", ruleIdx: "12", partIdx: "", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, "rule index is set, part index is not")
			}},
			{name: "part index without rule index", ruleIdx: "", partIdx: "34", assertErr: func(t *testing.T, err error) {
				require.EqualError(t, err, "part index is set, rule index is not")
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := iec.DecodePartInfoFromAttributes(tc.ruleIdx, tc.partIdx)
				tc.assertErr(t, err)
			})
		}
	})

	pi, err := iec.DecodePartInfoFromAttributes("12", "34")
	require.NoError(t, err)
	require.Equal(t, iec.PartInfo{RuleIndex: 12, Index: 34}, pi)
}

func TestObjectWithAttributes(t *testing.T) {
	var obj object.Object
	require.False(t, iec.ObjectWithAttributes(obj))

	otherAttrs := []object.Attribute{
		object.NewAttribute("k1", "v1"),
		object.NewAttribute("k2", "v2"),
		object.NewAttribute("__NEOFS__EXPIRATION_EPOCH", "123"),
	}

	obj.SetAttributes(otherAttrs...)
	require.False(t, iec.ObjectWithAttributes(obj))

	for _, attr := range []string{
		iec.AttributePartIdx,
		iec.AttributeRuleIdx,
		"__NEOFS__EC_any",
	} {
		obj.SetAttributes(append(otherAttrs, object.NewAttribute(attr, "any"))...)
		require.True(t, iec.ObjectWithAttributes(obj), attr)
	}
}
