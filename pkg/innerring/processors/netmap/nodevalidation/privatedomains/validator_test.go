package privatedomains

import (
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

type testNNS struct {
	mDomains map[string][]string

	staticErr error
}

// creates test NNS provider without any domains.
func newTestNNS() *testNNS {
	return &testNNS{
		mDomains: make(map[string][]string),
	}
}

// creates test NNS provider that always fails with the given error.
func newTestNNSWithStaticErr(err error) *testNNS {
	return &testNNS{
		staticErr: err,
	}
}

func (x *testNNS) registerDomain(domain string) {
	x.mDomains[domain] = nil
}

func (x *testNNS) addDomainRecord(domain string, record string) {
	x.mDomains[domain] = append(x.mDomains[domain], record)
}

func (x *testNNS) removeDomainRecord(domain string, record string) {
	recs, ok := x.mDomains[domain]
	if !ok {
		return
	}

	for i := 0; i < len(recs); i++ { // do not use range, slice is mutated inside
		if recs[i] == record {
			recs = append(recs[:i], recs[i+1:]...)
			i--
		}
	}

	x.mDomains[domain] = recs
}

func (x *testNNS) CheckDomainRecord(domainName string, record string) error {
	if x.staticErr != nil {
		return x.staticErr
	}

	recs, ok := x.mDomains[domainName]
	if !ok {
		return errors.New("missing domain")
	}

	if slices.Contains(recs, record) {
		return nil
	}

	return ErrMissingDomainRecord
}

func TestValidator_VerifyAndUpdate(t *testing.T) {
	const verifiedDomain = "nodes.some-org.neofs"
	const hNodeKey = "02a70577a832b338772c8cd07e7eaf526cae8d9b025a51b41671de5a4363eafe07"
	const nodeNeoAddress = "address=NZ1czz5gkEDamTg6Tiw6cxqp9Me1KLs8ae"
	const anyOtherNeoAddress = "address=NfMvD6WmBiCr4erfEnFFLs7jdj4Y5CM7nN"

	bNodeKey, err := hex.DecodeString(hNodeKey)
	require.NoError(t, err)

	var node netmap.NodeInfo
	node.SetPublicKey(bNodeKey)
	node.SetVerifiedNodesDomain(verifiedDomain)

	t.Run("unspecified verified nodes domain", func(t *testing.T) {
		var node netmap.NodeInfo
		require.Zero(t, node.VerifiedNodesDomain())

		v := New(newTestNNS())

		err := v.Verify(node)
		require.NoError(t, err)
	})

	t.Run("invalid node key", func(t *testing.T) {
		var node netmap.NodeInfo
		node.SetVerifiedNodesDomain(verifiedDomain)

		v := New(newTestNNS())

		node.SetPublicKey(nil)
		err := v.Verify(node)
		require.ErrorIs(t, err, errMissingNodeBinaryKey)

		node.SetPublicKey([]byte{})
		err = v.Verify(node)
		require.ErrorIs(t, err, errMissingNodeBinaryKey)
	})

	t.Run("other failure", func(t *testing.T) {
		anyErr := errors.New("any error")
		v := New(newTestNNSWithStaticErr(anyErr))

		err := v.Verify(node)
		require.ErrorIs(t, err, anyErr)
	})

	t.Run("missing domain", func(t *testing.T) {
		v := New(newTestNNS())

		err := v.Verify(node)
		require.Error(t, err)
		require.NotErrorIs(t, err, errAccessDenied)
	})

	t.Run("existing domain", func(t *testing.T) {
		nns := newTestNNS()
		v := New(nns)

		nns.registerDomain(verifiedDomain)

		err := v.Verify(node)
		require.ErrorIs(t, err, errAccessDenied)

		nns.addDomainRecord(verifiedDomain, anyOtherNeoAddress)
		err = v.Verify(node)
		require.ErrorIs(t, err, errAccessDenied)

		nns.addDomainRecord(verifiedDomain, nodeNeoAddress)
		err = v.Verify(node)
		require.NoError(t, err)

		nns.removeDomainRecord(verifiedDomain, nodeNeoAddress)
		err = v.Verify(node)
		require.ErrorIs(t, err, errAccessDenied)
	})
}
