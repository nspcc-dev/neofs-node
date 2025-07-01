# External SN Validator API Documentation

This document describes the API for integrating an external Storage Node (SN)
validator with NeoFS Inner Ring (IR).

## Overview

The external SN validator is a service that verifies the validity of a NeoFS
storage node by checking node information. The IR node sends a signed
request to the validator, which responds with a signed verification result.

## API Endpoint

- **URI:** `<url>` (as configured in `sn_validator.url`)
- **Method:** `POST`
- **Content-Type:** `application/json`

## Request

The request body is a JSON object containing a body field with node information and nonce, and its digital signature.

### Example Request

```json
{
  "body": {
    "node_info": "<JSON-encoded NodeInfo>",
    "nonce": 123456789
  },
  "signature": {
    "sign": "<bytes: signed body>"
  }
}
```

- `body`: Contains:
  - `node_info`: JSON-encoded [`netmap.NodeInfo`](https://pkg.go.dev/github.com/nspcc-dev/neofs-sdk-go/netmap#NodeInfo) object.
  - `nonce`: A random 64-bit unsigned integer to protect against replay attacks. Each request contains a unique nonce.
- `signature.sign`: Signature bytes over the entire `body` field (including both node_info and nonce), created by the node's private key.

## Response

The response is a JSON object containing the verification result as a raw JSON message in the `body`
field and a signature over the result in the `signature` field.

### Example Response

```json
{
  "body": {
    "verified": true,
    "details": "",
    "nonce": 123456789
  },
  "signature": {
    "sign": "<bytes: signed body>"
  }
}
```

- `body`: A JSON-encoded object with the following fields:
  - `verified`: Boolean indicating if the node is valid.
  - `details`: Optional string with additional information or error details (can be empty if verified is true).
  - `nonce`: The same nonce value received in the request, must match for the response to be accepted.
- `signature.sign`: Signature bytes over the raw `body` field.

## Signature Verification

- The IR node verifies the response signature using the public key corresponding
to the private key used for signing the request.
- The signature must be valid for the raw JSON-encoded `body` field.

## Error Handling

- If the response status code is not 200 OK, the IR node treats it as a verification failure.
- If the response is missing a signature or the signature is invalid, the IR node rejects the verification.
- If the nonce in the response doesn't match the nonce sent in the request, the IR node rejects the verification.
- If `body.verified` is false, the IR node treats the node as not verified and may use
`body.details` for error reporting.

## Example Workflow

1. IR node creates a request body containing the `NodeInfo` of the node that needs to be verified and a unique nonce.
2. IR node signs the complete request body (containing both node info and nonce) with its private key.
3. IR node sends a POST request to the configured endpoint with the signed request.
4. Validator checks the signature over the request body, verifies the node info, and responds with a signed
verification result in the `body` field, including the same nonce.
5. IR node verifies the response signature (over the raw `body` field), validates that the nonce matches,
and acts according to the `verified` and `details` fields.

## Configuration

Configure the validator endpoint in the IR config file:

```yaml
sn_validator:
  enabled: true
  url: http://localhost:8080/verify
```

## Notes

- The validator must use the same signature scheme as the IR node.
- The IR node expects the response signature to be generated using the same key as
the request signature and to cover the full `body` object.
- The validator should implement nonce checking to prevent replay attacks. The nonce value
  is included in the signed body to prevent tampering.
- The response must echo back the same nonce value that was sent in the request.
