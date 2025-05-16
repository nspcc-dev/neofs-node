package external

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Signature represents a digital signature of a signed message.
type Signature struct {
	Sign []byte `json:"sign"`
}

// RequestBody represents the body of a request sent
// to an external validator.
type RequestBody struct {
	NodeInfo netmap.NodeInfo `json:"node_info"`
	Nonce    uint64          `json:"nonce"`
}

// ResponseBody represents the body of the response from an external validator.
type ResponseBody struct {
	Verified bool   `json:"verified"`
	Details  string `json:"details"`
	Nonce    uint64 `json:"nonce"`
}

// SignedMessage represents a message with a digital signature.
type SignedMessage struct {
	Body      json.RawMessage `json:"body"`
	Signature *Signature      `json:"signature"`
}

// Verify validates with an external validator by sending
// a request to the specified endpoint with the node information
// and its corresponding digital signature.
//
// The external validator should be able to verify the node
// by using the public key from the signature and the node information
// in the request.
//
// If the verification is successful, the method returns nil.
// Otherwise, the method returns an error describing the reason.
func (v *Validator) Verify(n netmap.NodeInfo) error {
	err := v.verify(n)
	if err != nil {
		return fmt.Errorf("could not verify node by external validator: %w", err)
	}
	return nil
}

func (v *Validator) verify(n netmap.NodeInfo) error {
	nonce := generateNonce()
	reqBody, err := json.Marshal(RequestBody{
		NodeInfo: n,
		Nonce:    nonce,
	})
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}

	signedReq, err := json.Marshal(SignedMessage{
		Body: reqBody,
		Signature: &Signature{
			Sign: v.privateKey.Sign(reqBody),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal signed request: %w", err)
	}

	resp, err := v.client.Post(v.endpoint, "application/json", bytes.NewBuffer(signedReq))
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result SignedMessage
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	err = v.verifyResponse(result, v.privateKey.PublicKey())
	if err != nil {
		return fmt.Errorf("verify response signature: %w", err)
	}

	var respBody ResponseBody
	err = json.Unmarshal(result.Body, &respBody)
	if err != nil {
		return fmt.Errorf("unmarshal result: %w", err)
	}

	if respBody.Nonce != nonce {
		return fmt.Errorf("nonce mismatch: expected %d, got %d", nonce, respBody.Nonce)
	}

	if !respBody.Verified {
		return fmt.Errorf("not verified: %s", respBody.Details)
	}
	return nil
}

func (v *Validator) verifyResponse(resp SignedMessage, pubKey *keys.PublicKey) error {
	if resp.Signature == nil {
		return errors.New("missing response signature")
	}

	if !pubKey.Verify(resp.Signature.Sign, hash.Sha256(resp.Body).BytesBE()) {
		return errors.New("invalid response signature")
	}
	return nil
}

// generateNonce creates a random nonce value to protect against replay attacks.
// It generates an 8-byte random value and encodes it as uint64.
func generateNonce() uint64 {
	nonce := make([]byte, 8)
	_, _ = rand.Read(nonce)
	return binary.LittleEndian.Uint64(nonce)
}
