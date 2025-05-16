package external

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

		var nodeInfo netmap.NodeInfo
		err = json.Unmarshal(req.NodeInfo, &nodeInfo)
		require.NoError(t, err)

		require.Len(t, nodeInfo.GetAttributes(), 1)
		require.Equal(t, nodeInfo.GetAttributes(), attrs)

		pubKey := (*neofsecdsa.PublicKey)(&nodePrivateKey.PublicKey)
		require.True(t, pubKey.Verify(req.NodeInfo, req.Signature.Sign))

		w.WriteHeader(http.StatusOK)

		result := Body{
			Verified: true,
		}
		marshalledResult, err := json.Marshal(result)
		require.NoError(t, err)
		var baseSign neofscrypto.Signature
		err = baseSign.Calculate(neofsecdsa.Signer(*nodePrivateKey), marshalledResult)
		require.NoError(t, err)

		err = json.NewEncoder(w).Encode(SignedResponse{
			Result:    result,
			Signature: &Signature{Sign: baseSign.Value()},
		})
		require.NoError(t, err)
	}))
	defer server.Close()

	validator := New(server.URL+"/verify", nodePrivateKey)

	var nodeInfo netmap.NodeInfo
	nodeInfo.SetAttributes(attrs)
	err = validator.Verify(nodeInfo)
	require.NoError(t, err)
}

func TestAPIClient_Verify_Fail(t *testing.T) {
	nodePrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	attrs := [][2]string{{"key", "test"}}

	t.Run("bad response status code", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		validator := New(server.URL, nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected status code: 400")
	})

	t.Run("invalid response signature", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			err := json.NewEncoder(w).Encode(SignedResponse{
				Result: Body{
					Verified: true,
				},
				Signature: &Signature{Sign: []byte("invalid")},
			})
			require.NoError(t, err)
		}))
		defer server.Close()

		validator := New(server.URL, nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid response signature")
	})

	t.Run("missing response signature", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			err := json.NewEncoder(w).Encode(SignedResponse{
				Result: Body{
					Verified: true,
				},
			})
			require.NoError(t, err)
		}))
		defer server.Close()

		validator := New(server.URL, nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing response signature")
	})

	t.Run("verification failed", func(t *testing.T) {
		details := "verification failed"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			result := Body{
				Verified: false,
				Details:  details,
			}
			marshalledResult, err := json.Marshal(result)
			require.NoError(t, err)
			var baseSign neofscrypto.Signature
			err = baseSign.Calculate(neofsecdsa.Signer(*nodePrivateKey), marshalledResult)
			require.NoError(t, err)
			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(SignedResponse{
				Result:    result,
				Signature: &Signature{Sign: baseSign.Value()},
			})
			require.NoError(t, err)
		}))
		defer server.Close()

		validator := New(server.URL, nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("not verified: %s", details))
	})

	t.Run("invalid server url", func(t *testing.T) {
		validator := New("http://invalid-url", nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "send request")
	})
}
