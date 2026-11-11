package container

import (
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	nnscore "github.com/nspcc-dev/neofs-node/pkg/core/nns"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

type testAlphabetState struct{}

func (testAlphabetState) IsAlphabet() bool { return true }

type testChainTime struct{}

func (testChainTime) Now() time.Time { return time.Now() }

func TestProcessCreateContainerRequestEACL(t *testing.T) {
	owner := usertest.User()

	var policy netmap.PlacementPolicy
	var rep netmap.ReplicaDescriptor
	rep.SetNumberOfObjects(1)
	policy.SetReplicas([]netmap.ReplicaDescriptor{rep})

	nonce := uuid.New()
	cnrInfo := containerrpc.ContainerInfo{
		Version:       &containerrpc.ContainerAPIVersion{Major: big.NewInt(2), Minor: big.NewInt(22)},
		Owner:         owner.ID.ScriptHash(),
		Nonce:         nonce[:],
		BasicACL:      big.NewInt(int64(acl.PublicRWExtended.Bits())),
		StoragePolicy: policy.Marshal(),
	}

	cnr, err := cntClient.ContainerFromStruct(cnrInfo)
	require.NoError(t, err)
	id := cid.NewFromMarshalledContainer(cnr.Marshal())

	table := anyValidEACL()
	table.SetCID(id)
	table.SetRecords([]eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationGet,
		[]eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)})})
	tableBytes := table.Marshal()

	newProcessor := func(t *testing.T) (*Processor, *observer.ObservedLogs) {
		cnrCli, err := cntClient.NewFromMorph(&client.Client{}, [20]byte{})
		require.NoError(t, err)
		core, logs := observer.New(zap.DebugLevel)
		return &Processor{
			log:           zap.New(core),
			alphabetState: testAlphabetState{},
			cnrClient:     cnrCli,
			chainTime:     testChainTime{},
			resolver:      nnscore.NewResolver(cnrCli.Morph()),
		}, logs
	}

	makeRequest := func(t *testing.T, reqSigner, eACLSigner neofscrypto.Signer, st *sessionv2.Token) containerEvent.CreateContainerV2Request {
		eACLSig, err := eACLSigner.Sign(tableBytes)
		require.NoError(t, err)
		eACLPub := neofscrypto.PublicKeyBytes(eACLSigner.Public())
		cnrSig, err := reqSigner.Sign(cnr.Marshal())
		require.NoError(t, err)

		apiReq := &protocontainer.PutRequest{
			Body: &protocontainer.PutRequest_Body{
				Container: cnr.ProtoMessage(),
				Signature: &refs.SignatureRFC6979{
					Key:  neofscrypto.PublicKeyBytes(reqSigner.Public()),
					Sign: cnrSig,
				},
				Eacl: table.ProtoMessage(),
				EaclSignature: &refs.SignatureRFC6979{
					Key:  eACLPub,
					Sign: eACLSig,
				},
			},
			MetaHeader: &protosession.RequestMetaHeader{
				Version:        version.Current().ProtoMessage(),
				ValidUntilTime: uint64(time.Now().Add(time.Minute).Unix()),
			},
		}
		var stBytes []byte
		if st != nil {
			apiReq.MetaHeader.SessionTokenV2 = st.ProtoMessage()
			stBytes = st.Marshal()
		}
		apiReq.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(reqSigner, apiReq, nil)
		require.NoError(t, err)

		origReq, err := proto.Marshal(apiReq)
		require.NoError(t, err)

		return containerEvent.CreateContainerV2Request{
			Container: cnrInfo,
			EACLTable: &containerEvent.PutContainerEACLRequest{
				PutContainerEACLParams: fschaincontracts.PutContainerEACLParams{
					EACL:               tableBytes,
					InvocationScript:   eACLSig,
					VerificationScript: eACLPub,
					SessionToken:       stBytes,
				},
			},
			OriginalRequest: origReq,
		}
	}

	// zero FS chain client makes approval fail, its error log marks that all checks passed
	const approveFailedMsg = "could not approve put container"

	requireApproved := func(t *testing.T, logs *observer.ObservedLogs) {
		require.Zero(t, logs.FilterMessage("additional eACL with container creation check failed").Len())
		require.Equal(t, 1, logs.FilterMessage(approveFailedMsg).Len())
	}
	requireEACLRejected := func(t *testing.T, logs *observer.ObservedLogs, reason string) {
		entries := logs.FilterMessage("additional eACL with container creation check failed").All()
		require.Len(t, entries, 1)
		require.Contains(t, entries[0].ContextMap()["error"], reason)
		require.Zero(t, logs.FilterMessage(approveFailedMsg).Len())
	}

	t.Run("signed by owner", func(t *testing.T) {
		cp, logs := newProcessor(t)
		cp.processCreateContainerRequest(makeRequest(t, owner.RFC6979, owner.RFC6979, nil))
		requireApproved(t, logs)
	})

	t.Run("signed by other key", func(t *testing.T) {
		cp, logs := newProcessor(t)
		cp.processCreateContainerRequest(makeRequest(t, owner.RFC6979, usertest.User().RFC6979, nil))
		requireEACLRejected(t, logs, "owner mismatches signature")
	})

	t.Run("session", func(t *testing.T) {
		subject := usertest.User()

		newToken := func(t *testing.T, verbs []sessionv2.Verb) *sessionv2.Token {
			var st sessionv2.Token
			st.SetVersion(sessionv2.TokenCurrentVersion)
			now := time.Now()
			st.SetIat(now)
			st.SetNbf(now)
			st.SetExp(now.Add(time.Minute))
			sCtx, err := sessionv2.NewContext(cid.ID{}, verbs)
			require.NoError(t, err)
			require.NoError(t, st.SetContexts([]sessionv2.Context{sCtx}))
			require.NoError(t, st.AddSubject(sessionv2.NewTargetUser(subject.ID)))
			require.NoError(t, st.Sign(owner))
			return &st
		}
		allVerbs := []sessionv2.Verb{sessionv2.VerbContainerPut, sessionv2.VerbContainerSetEACL}

		t.Run("with SetEACL", func(t *testing.T) {
			cp, logs := newProcessor(t)
			cp.processCreateContainerRequest(makeRequest(t, subject.RFC6979, subject.RFC6979, newToken(t, allVerbs)))
			requireApproved(t, logs)
		})

		t.Run("without SetEACL", func(t *testing.T) {
			st := newToken(t, []sessionv2.Verb{sessionv2.VerbContainerPut})
			cp, logs := newProcessor(t)
			cp.processCreateContainerRequest(makeRequest(t, subject.RFC6979, subject.RFC6979, st))
			requireEACLRejected(t, logs, "wrong container ID")
		})

		t.Run("signed by other key", func(t *testing.T) {
			cp, logs := newProcessor(t)
			cp.processCreateContainerRequest(makeRequest(t, subject.RFC6979, usertest.User().RFC6979, newToken(t, allVerbs)))
			requireEACLRejected(t, logs, "session v2 token is not for operation signer")
		})
	})
}
