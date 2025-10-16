package object

import (
	"fmt"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestFormatValidator_Validate_EC(t *testing.T) {
	const ruleIdx = 1

	irule := iec.Rule{DataPartNum: 12, ParityPartNum: 4}
	rule := netmap.NewECRule(uint32(irule.DataPartNum), uint32(irule.ParityPartNum))
	cnrID := cidtest.ID()
	otherCnrID := cidtest.OtherID(cnrID)

	var policy netmap.PlacementPolicy
	policy.SetECRules([]netmap.ECRule{
		netmap.NewECRule(3, 1),
		rule,
		netmap.NewECRule(6, 3),
	})

	var cnr container.Container
	cnr.SetPlacementPolicy(policy)

	cnrs := newMockContainers()
	cnrs.setContainer(cnrID, cnr)
	cnrs.setContainer(otherCnrID, cnr)

	v := NewFormatValidator(nil, nil, cnrs)

	creator := usertest.User()
	owner := creator.UserID()
	otherOwner := usertest.OtherID(owner)

	const parentPldLen = 4 << 10
	parentPld := testutil.RandByteSlice(parentPldLen)

	var parent object.Object
	parent.SetContainerID(cnrID)
	parent.SetOwner(owner)
	parent.SetPayloadSize(parentPldLen)
	parent.SetPayload(parentPld)
	parent.SetAttributes(
		object.NewAttribute("attr1", "val1"),
		object.NewAttribute("attr2", "val2"),
	)
	require.NoError(t, parent.SetVerificationFields(creator))

	parts, err := iec.Encode(irule, parentPld)
	require.NoError(t, err)

	var ecParts []object.Object
	for i := range parts {
		obj, err := iec.FormObjectForECPart(creator, parent, parts[i], iec.PartInfo{
			RuleIndex: ruleIdx,
			Index:     i,
		})
		require.NoError(t, err)

		ecParts = append(ecParts, obj)
	}

	corruptParent := func(t *testing.T, obj object.Object, f func(*object.Object)) object.Object {
		var cp object.Object
		obj.CopyTo(&cp)
		f(&cp)
		require.NoError(t, cp.SetVerificationFields(creator))
		return cp
	}

	corruptPart := func(t *testing.T, f func(*object.Object)) object.Object {
		return corruptParent(t, ecParts[0], f)
	}

	for _, tc := range []struct {
		name          string
		err           string
		corruptParent func(*object.Object)
		corruptPart   func(*object.Object)
	}{
		{name: "missing parent header", err: "invalid regular EC part object: missing parent header", corruptPart: func(obj *object.Object) {
			obj.SetParent(nil)
		}},
		{name: "parent with EC attribute", err: "parent object has EC attribute __NEOFS__EC_FOO", corruptParent: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute("__NEOFS__EC_FOO", "any"),
			)
		}, corruptPart: func(obj *object.Object) {}},
		{name: "unknown object type", err: "unsupported object type 6", corruptPart: func(obj *object.Object) {
			obj.SetType(6)
		}},
		{name: "negative object type", err: "unsupported object type -1", corruptPart: func(obj *object.Object) {
			obj.SetType(-1)
		}},
		{name: "mixed EC and non-EC attributes (non-EC first)", err: "mix of EC (__NEOFS__EC_RULE_IDX) and non-EC (foo) attributes", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute("foo", "bar"),
				object.NewAttribute(iec.AttributeRuleIdx, "1"),
				object.NewAttribute(iec.AttributePartIdx, "0"),
			)
		}},
		{name: "mixed EC and non-EC attributes (EC first)", err: "mix of EC (__NEOFS__EC_PART_IDX) and non-EC (foo) attributes", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributePartIdx, "0"),
				object.NewAttribute("foo", "bar"),
				object.NewAttribute(iec.AttributeRuleIdx, "1"),
			)
		}},
		{name: "proto version mismatch", err: "invalid regular EC part object: diff proto version in parent (v1.2) and part (v3.4)", corruptParent: func(obj *object.Object) {
			v := version.New(1, 2)
			obj.SetVersion(&v)
		}, corruptPart: func(obj *object.Object) {
			v := version.New(3, 4)
			obj.SetVersion(&v)
		}},
		{name: "container mismatch", err: fmt.Sprintf("invalid regular EC part object: diff container in parent (%s) and part (%s)",
			cnrID, otherCnrID), corruptParent: func(obj *object.Object) {
			obj.SetContainerID(cnrID)
		}, corruptPart: func(obj *object.Object) {
			obj.SetContainerID(otherCnrID)
		}},
		{name: "owner mismatch", err: fmt.Sprintf("invalid regular EC part object: diff owner in parent (%s) and part (%s)",
			owner, otherOwner), corruptParent: func(obj *object.Object) {
			obj.SetOwner(owner)
		}, corruptPart: func(obj *object.Object) {
			obj.SetOwner(otherOwner)
		}},
		{name: "owner mismatch", err: fmt.Sprintf("invalid regular EC part object: diff creation epoch in parent (%d) and part (%d)",
			1, 2), corruptParent: func(obj *object.Object) {
			obj.SetCreationEpoch(1)
		}, corruptPart: func(obj *object.Object) {
			obj.SetCreationEpoch(2)
		}},
		{name: "homomorphic hash mismatch", err: fmt.Sprintf("invalid regular EC part object: diff homomorphic hash presence in parent (%t) and part (%t)",
			true, false), corruptParent: func(obj *object.Object) {
			obj.SetPayloadHomomorphicHash(checksumtest.Checksum())
		}, corruptPart: func(obj *object.Object) {}},
		{name: "missing EC attributes", err: "missing EC attributes in regular object", corruptPart: func(obj *object.Object) {
			obj.SetAttributes()
		}},
		{name: "non-int rule index attribute", err: "invalid regular EC part object: unavailable part info: invalid index attribute __NEOFS__EC_RULE_IDX: " +
			`strconv.Atoi: parsing "foo": invalid syntax`, corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "foo"),
				object.NewAttribute(iec.AttributePartIdx, "0"),
			)
		}},
		{name: "non-int part index attribute", err: "invalid regular EC part object: unavailable part info: invalid index attribute __NEOFS__EC_PART_IDX: " +
			`strconv.Atoi: parsing "foo": invalid syntax`, corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "1"),
				object.NewAttribute(iec.AttributePartIdx, "foo"),
			)
		}},
		{name: "negative rule index attribute", err: "invalid regular EC part object: unavailable part info: invalid index attribute __NEOFS__EC_RULE_IDX: " +
			"negative value -1", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "-1"),
				object.NewAttribute(iec.AttributePartIdx, "0"),
			)
		}},
		{name: "negative part index attribute", err: "invalid regular EC part object: unavailable part info: invalid index attribute __NEOFS__EC_PART_IDX: " +
			"negative value -1", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "1"),
				object.NewAttribute(iec.AttributePartIdx, "-1"),
			)
		}},
		{name: "too big rule index", err: "invalid regular EC part object: rule index attribute (3) overflows total number of rules in policy (3)", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "3"),
				object.NewAttribute(iec.AttributePartIdx, "0"),
			)
		}},
		{name: "too big part index", err: "invalid regular EC part object: part index attribute (16) overflows total number of parts in policy (16)", corruptPart: func(obj *object.Object) {
			obj.SetAttributes(
				object.NewAttribute(iec.AttributeRuleIdx, "1"),
				object.NewAttribute(iec.AttributePartIdx, "16"),
			)
		}},
		{name: "wrong payload len", err: "invalid regular EC part object: wrong part payload len: expected 342, got 343, parent 4096", corruptPart: func(obj *object.Object) {
			obj.SetPayloadSize(343)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.corruptParent != nil {
				parent := corruptParent(t, parent, tc.corruptParent)
				cp := corruptPart(t, func(obj *object.Object) {
					obj.SetParent(&parent)
					tc.corruptPart(obj)
				})
				require.EqualError(t, v.Validate(&cp, false), tc.err)
				return
			}

			cp := corruptPart(t, tc.corruptPart)
			require.EqualError(t, v.Validate(&cp, false), tc.err)
		})
	}

	t.Run("tombstone", func(t *testing.T) {
		cp := corruptPart(t, func(obj *object.Object) { obj.SetType(object.TypeTombstone) })
		require.EqualError(t, v.Validate(&cp, false), "TOMBSTONE object with EC attribute __NEOFS__EC_RULE_IDX")

		cp = corruptParent(t, parent, func(obj *object.Object) { obj.SetType(object.TypeTombstone) })
		require.NoError(t, v.Validate(&cp, false))
	})

	t.Run("lock", func(t *testing.T) {
		cp := corruptPart(t, func(obj *object.Object) { obj.SetType(object.TypeLock) })
		require.EqualError(t, v.Validate(&cp, false), "LOCK object with EC attribute __NEOFS__EC_RULE_IDX")

		cp = corruptParent(t, parent, func(obj *object.Object) { obj.SetType(object.TypeLock) })
		require.NoError(t, v.Validate(&cp, false))
	})

	t.Run("blank part", func(t *testing.T) {
		for i := range ecParts {
			require.EqualError(t, v.Validate(&ecParts[i], true), "blank object with EC attributes")
		}
	})

	for i := range ecParts {
		require.NoError(t, v.Validate(&ecParts[i], false))
	}
}
