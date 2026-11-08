package policer

import (
	"context"
	"testing"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeMaxSizeSource uint64

func (f fakeMaxSizeSource) MaxObjectSize() uint64 { return uint64(f) }

// Regression test for M15: checkECParts sizes a buffer allocation
// (buf := make([]byte, int(partLen)*totalParts) in the caller, and the
// equivalent needLen computation in fetchObjectFromECRule) directly from an
// EC parent header's declared PayloadSize - a value received from another EC
// container member while checking that container's parts, never independently
// verified before being used to size an allocation. A malicious or compromised
// member (or an attacker who placed themselves as a member of the same
// container) could declare an arbitrarily large payload size there to force a
// huge allocation on the checking node, regardless of what it actually stores.
func TestPolicer_checkECParts_RejectsOversizedParentPayload(t *testing.T) {
	cnr := cidtest.ID()
	parent := oidtest.ID()
	localPartID := oidtest.ID()

	rule := iec.Rule{DataPartNum: 2, ParityPartNum: 0}
	nodes := testutil.Nodes(2) // nodes[0]: local; nodes[1]: remote, serves part 1

	mockNet := newMockNetwork()
	mockNet.pubKey = nodes[0].PublicKey()
	mockNet.setObjectNodesECResult(cnr, parent, [][]netmap.NodeInfo{nodes}, []iec.Rule{rule})

	const configuredMax = 1 << 20 // 1 MiB: arbitrary, just needs to be exceeded below

	newPolicerWithMaxSize := func(maxSize MaxSizeSource) (*Policer, *testutil.LogBuffer) {
		l, lb := testutil.NewBufferedLogger(t, zap.DebugLevel)
		p := New(neofscryptotest.Signer(),
			WithNetwork(mockNet),
			WithLogger(l),
			WithHeadTimeout(time.Second),
			WithMaxSizeSource(maxSize),
		)

		conns := newMockAPIConnections()

		var remoteHdr object.Object
		var attrRule, attrPart object.Attribute
		attrRule.SetKey(object.AttributeECRuleIndex)
		attrRule.SetValue("0")
		attrPart.SetKey(object.AttributeECPartIndex)
		attrPart.SetValue("1")
		remoteHdr.SetAttributes(attrRule, attrPart)

		// SetParent derives GetParentID from the parent object's own ID, so
		// the parent's ID must be set before attaching it - setting it
		// afterwards (e.g. via a separate SetParentID call) would be
		// silently clobbered back to zero.
		var maliciousParent object.Object
		maliciousParent.SetID(parent)
		maliciousParent.SetPayloadSize(configuredMax * 1000) // far beyond configuredMax
		remoteHdr.SetParent(&maliciousParent)

		conns.setHeadResult(nodes[1], oid.NewAddress(cnr, parent), remoteHdr, nil)
		p.apiConns = conns

		return p, lb
	}

	t.Run("oversized parent payload size is rejected before any allocation", func(t *testing.T) {
		p, lb := newPolicerWithMaxSize(fakeMaxSizeSource(configuredMax))

		p.checkECParts(context.Background(), cnr, parent, rule, 0, 0, localPartID)

		lb.AssertContainsMsg(zap.ErrorLevel, "EC parent payload size declared by peer rejected")
	})

	t.Run("missing max size source is rejected, not nil-dereferenced", func(t *testing.T) {
		p, lb := newPolicerWithMaxSize(nil)

		require.NotPanics(t, func() {
			p.checkECParts(context.Background(), cnr, parent, rule, 0, 0, localPartID)
		})
		lb.AssertContainsMsg(zap.ErrorLevel, "EC parent payload size declared by peer rejected")
	})
}
