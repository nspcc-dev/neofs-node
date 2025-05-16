# External SN Validator API Documentation

This document describes the API for integrating an external Storage Node (SN)
validator with NeoFS Inner Ring (IR).

## Overview

The external SN validator is a service that verifies the validity of a NeoFS
storage node by checking node information. The IR node sends a signed
request to the validator, which responds with a signed verification result.

## API Endpoint

- **URI:** `<address>` (as configured in `sn_validator.address`)
- **Method:** `POST`
- **Content-Type:** `application/json`

## Request

The request body is a JSON object containing the node information and its digital signature.

### Example Request

```json
{
  "node_info": <bytes: marshaled NodeInfo>,
  "signature": {
    "sign": <bytes: signed node_info>
  }
}
```

- `node_info`: JSON-encoded [`netmap.NodeInfo`](https://pkg.go.dev/github.com/nspcc-dev/neofs-sdk-go/netmap#NodeInfo)
object.
- `signature.sign`: Signature bytes (ECDSA-SHA512) over the `node_info` field, created by the node's private key.

## Response

The response is a JSON object containing the verification result in a `result`
field and a signature over the result in a `signature` field.

### Example Response

```json
{
  "result": {
    "verified": true,
    "details": ""
  },
  "signature": {
    "sign": <bytes: signature over result field>
  }
}
```

- `result`: An object with the following fields:
  - `verified`: Boolean indicating if the node is valid.
  - `details`: Optional string with additional information or error details (can be empty if verified is true).
- `signature.sign`: Signature bytes (ECDSA-SHA512) over the JSON-encoded `result` field.

## Signature Verification

- The IR node verifies the response signature using the public key corresponding
to the private key used for signing the request.
- The signature must be valid for the JSON-encoded `result` field (not just the `verified` value).

## Error Handling

- If the response status code is not 200 OK, the IR node treats it as a verification failure.
- If the response is missing a signature or the signature is invalid, the IR node rejects the verification.
- If `result.verified` is false, the IR node treats the node as not verified and may use
`result.details` for error reporting.

## Example Workflow

1. IR node marshals `NodeInfo` of the node that needs to be verified and signs it with its private key.
2. IR node sends a POST request to config URI with the signed node info.
3. Validator checks the signature and node info, then responds with a signed
verification result in the `result` field.
4. IR node verifies the response signature (over the `result` field) and acts
according to the `verified` and `details` fields.

## Configuration

Configure the validator endpoint in the IR config file:

```yaml
sn_validator:
  enabled: true
  address: http://localhost:8080/verify
```

## Notes

- The validator must use the same signature scheme as the IR node (ECDSA-SHA512).
- The IR node expects the response signature to be generated using the same key as
the request signature and to cover the full `result` object.
