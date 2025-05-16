package external

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Signature represents a digital signature with
// a corresponding public key verifying the signed data.
type Signature struct {
	PublicKey []byte `json:"key"`
	Sign      []byte `json:"sign"`
}

// SignedRequest represents a request containing node information
// and its corresponding digital signature.
type SignedRequest struct {
	NodeInfo  netmap.NodeInfo `json:"node_info"`
	Signature *Signature      `json:"signature"`
}

// SignedResponse represents the result of a verification
// process from an external validator.
// Verified indicates whether the verification was successful.
type SignedResponse struct {
	Verified  bool       `json:"verified"`
	Signature *Signature `json:"signature"`
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
	sign, err := v.signData(n.Marshal())
	if err != nil {
		return err
	}

	signedBody, err := json.Marshal(SignedRequest{
		NodeInfo:  n,
		Signature: sign,
	})
	if err != nil {
		return fmt.Errorf("marshal signed request: %w", err)
	}

	resp, err := v.client.Post(v.endpoint+"/verify", "application/json", bytes.NewBuffer(signedBody))
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result SignedResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	err = v.verifyResponse(result)
	if err != nil {
		return fmt.Errorf("verify response signature: %w", err)
	}

	if !result.Verified {
		return fmt.Errorf("not verified")
	}
	return nil
}

func (v *Validator) signData(data []byte) (sign *Signature, err error) {
	var baseSign neofscrypto.Signature

	err = baseSign.Calculate(neofsecdsa.Signer(*v.privateKey), data)
	if err != nil {
		return nil, fmt.Errorf("calculate signature: %w", err)
	}

	return &Signature{
		PublicKey: baseSign.PublicKeyBytes(),
		Sign:      baseSign.Value(),
	}, nil
}

func (v *Validator) verifyResponse(resp SignedResponse) error {
	if resp.Signature == nil {
		return errors.New("missing response signature")
	}

	var pubKey neofsecdsa.PublicKey
	if err := pubKey.Decode(resp.Signature.PublicKey); err != nil {
		return fmt.Errorf("decode public key from signature: %w", err)
	}

	sig := neofscrypto.NewSignature(neofscrypto.ECDSA_SHA512, &pubKey, resp.Signature.Sign)

	if !sig.Verify([]byte(strconv.FormatBool(resp.Verified))) {
		return errors.New("invalid response signature")
	}
	return nil
}
