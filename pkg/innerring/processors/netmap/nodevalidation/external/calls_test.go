package external

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestAPIClient_Verify(t *testing.T) {
	nodePrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	attrs := [][2]string{{"key", "test"}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/verify" {
			t.Errorf("Expected path /verify, got %s", r.URL.Path)
		}
		var req SignedRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		require.Len(t, req.NodeInfo.GetAttributes(), 1)
		require.Equal(t, req.NodeInfo.GetAttributes(), attrs)

		pubKey := (*neofsecdsa.PublicKey)(&nodePrivateKey.PublicKey)
		require.True(t, pubKey.Verify(req.NodeInfo.Marshal(), req.Signature.Sign))

		w.WriteHeader(http.StatusOK)

		var serverPrivateKey *ecdsa.PrivateKey
		serverPrivateKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		var baseSign neofscrypto.Signature
		err = baseSign.Calculate(neofsecdsa.Signer(*serverPrivateKey), []byte(strconv.FormatBool(true)))
		require.NoError(t, err)

		err = json.NewEncoder(w).Encode(SignedResponse{
			Verified:  true,
			Signature: &Signature{PublicKey: baseSign.PublicKeyBytes(), Sign: baseSign.Value()},
		})
		require.NoError(t, err)
	}))
	defer server.Close()

	validator := New(server.URL, nodePrivateKey)

	var nodeInfo netmap.NodeInfo
	nodeInfo.SetAttributes(attrs)
	err = validator.Verify(nodeInfo)
	require.NoError(t, err)
}
