package session

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math"
	"testing"
	"time"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

type recordingKeyStorage struct {
	expirations []uint64
}

func (s *recordingKeyStorage) Store(_ ecdsa.PrivateKey, exp uint64) error {
	s.expirations = append(s.expirations, exp)
	return nil
}

type fixedTime struct{ time.Time }

func (t fixedTime) Now() time.Time { return t.Time }

type fixedEpoch uint64

func (e fixedEpoch) CurrentEpoch() uint64 { return uint64(e) }

func TestCreateExpiration(t *testing.T) {
	signer, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Unix(1_700_000_000, 0)

	for _, tc := range []struct {
		name         string
		current, exp uint64
		wantErr      string
	}{
		{name: "past expiration", current: 10, exp: 9, wantErr: "precedes the current epoch"},
		{name: "expiration at the current epoch", current: 10, exp: 10},
		{name: "expiration at the limit", current: 10, exp: 10 + maxSessionLifetime},
		{name: "expiration above the limit", current: 10, exp: 11 + maxSessionLifetime, wantErr: "exceeds the maximum session lifetime"},
		{name: "expiration far above the limit", current: 10, exp: math.MaxUint64, wantErr: "exceeds the maximum session lifetime"},
		{name: "valid expiration near the uint64 ceiling", current: math.MaxUint64 - 1, exp: math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := new(recordingKeyStorage)
			srv := New(signer, store, fixedTime{now}, fixedEpoch(tc.current))
			req := &protosession.CreateRequest{
				Body: &protosession.CreateRequest_Body{
					OwnerId:    user.NewFromECDSAPublicKey(signer.PublicKey).ProtoMessage(),
					Expiration: tc.exp,
				},
				MetaHeader: &protosession.RequestMetaHeader{
					Version:        version.Current().ProtoMessage(),
					ValidUntilTime: uint64(now.Unix() + 60),
				},
			}
			var err error
			req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(*signer), req, nil)
			require.NoError(t, err)

			resp, err := srv.Create(t.Context(), req)
			require.NoError(t, err)
			respErr := apistatus.ToError(resp.GetMetaHeader().GetStatus())
			if tc.wantErr != "" {
				var badRequest *apistatus.BadRequest
				require.ErrorAs(t, respErr, &badRequest)
				require.ErrorContains(t, respErr, tc.wantErr)
				require.Nil(t, resp.GetBody())
				require.Empty(t, store.expirations)
				return
			}
			require.NoError(t, respErr)
			require.NotEmpty(t, resp.GetBody().GetSessionKey())
			require.Equal(t, []uint64{tc.exp}, store.expirations)
		})
	}
}
