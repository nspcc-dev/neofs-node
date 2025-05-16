package external

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestValidator_Verify(t *testing.T) {
	nodePrivateKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	attrs := [][2]string{{"key", "test"}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/verify" {
			t.Errorf("Expected path /verify, got %s", r.URL.Path)
		}
		var req SignedMessage
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		var reqBody RequestBody
		err = json.Unmarshal(req.Body, &reqBody)
		require.NoError(t, err)

		require.Len(t, reqBody.NodeInfo.GetAttributes(), 1)
		require.Equal(t, reqBody.NodeInfo.GetAttributes(), attrs)

		require.NotZero(t, reqBody.Nonce)

		pubKey := nodePrivateKey.PublicKey()
		require.True(t, pubKey.Verify(req.Signature.Sign, hash.Sha256(req.Body).BytesBE()))

		w.WriteHeader(http.StatusOK)

		result := ResponseBody{
			Verified: true,
			Nonce:    reqBody.Nonce,
		}
		var marshalledResult []byte
		marshalledResult, err = json.Marshal(result)
		require.NoError(t, err)

		err = json.NewEncoder(w).Encode(SignedMessage{
			Body: marshalledResult,
			Signature: &Signature{
				Sign: nodePrivateKey.Sign(marshalledResult),
			},
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

func TestNonceUniqueness(t *testing.T) {
	nodePrivateKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	attrs := [][2]string{{"key", "test"}}

	var (
		nonceMu sync.Mutex
		nonces  = make(map[uint64]bool)
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SignedMessage
		var err error
		err = json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		var reqBody RequestBody
		err = json.Unmarshal(req.Body, &reqBody)
		require.NoError(t, err)

		nonceMu.Lock()
		_, seen := nonces[reqBody.Nonce]
		require.False(t, seen)
		nonces[reqBody.Nonce] = true
		nonceMu.Unlock()

		result := ResponseBody{
			Verified: true,
			Nonce:    reqBody.Nonce,
		}
		var marshalledResult []byte
		marshalledResult, err = json.Marshal(result)
		require.NoError(t, err)

		sig := &Signature{
			Sign: nodePrivateKey.Sign(marshalledResult),
		}

		err = json.NewEncoder(w).Encode(SignedMessage{
			Body:      marshalledResult,
			Signature: sig,
		})
		require.NoError(t, err)
	}))
	defer server.Close()

	validator := New(server.URL, nodePrivateKey)

	count := 10
	for range count {
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.NoError(t, err)
	}

	require.Equal(t, count, len(nonces))
}

func TestAPIClient_Verify_Fail(t *testing.T) {
	nodePrivateKey, err := keys.NewPrivateKey()
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
			result := ResponseBody{
				Verified: true,
			}
			marshalledResult, err := json.Marshal(result)
			require.NoError(t, err)
			err = json.NewEncoder(w).Encode(SignedMessage{
				Body:      marshalledResult,
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
			result := ResponseBody{
				Verified: true,
			}
			marshalledResult, err := json.Marshal(result)
			require.NoError(t, err)
			err = json.NewEncoder(w).Encode(SignedMessage{
				Body: marshalledResult,
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
			var req SignedMessage
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			var reqBody RequestBody
			err = json.Unmarshal(req.Body, &reqBody)
			require.NoError(t, err)

			result := ResponseBody{
				Verified: false,
				Nonce:    reqBody.Nonce,
				Details:  details,
			}
			marshalledResult, err := json.Marshal(result)
			require.NoError(t, err)

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(SignedMessage{
				Body:      marshalledResult,
				Signature: &Signature{Sign: nodePrivateKey.Sign(marshalledResult)},
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

	t.Run("nonce mismatch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req SignedMessage
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			var reqBody RequestBody
			err = json.Unmarshal(req.Body, &reqBody)
			require.NoError(t, err)

			result := ResponseBody{
				Verified: true,
				Nonce:    reqBody.Nonce + 1,
			}
			marshalledResult, err := json.Marshal(result)
			require.NoError(t, err)

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(SignedMessage{
				Body:      marshalledResult,
				Signature: &Signature{Sign: nodePrivateKey.Sign(marshalledResult)},
			})
			require.NoError(t, err)
		}))
		defer server.Close()

		validator := New(server.URL, nodePrivateKey)
		var nodeInfo netmap.NodeInfo
		nodeInfo.SetAttributes(attrs)
		err = validator.Verify(nodeInfo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nonce mismatch")
	})
}
