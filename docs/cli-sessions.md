# How NeoFS CLI uses session mechanism of the NeoFS

## Overview

NeoFS sessions implement a mechanism for issuing a power of attorney by one
party to another. A trusted party can provide a so-called session token as
proof of the right to act on behalf of another member of the network. The
client of operations carried out with such a token will be the user who opened
the session. The token contains information which limits power of attorney like
action context or lifetime.

The client confirms trust in a third party by signing its public (session) key
with his private key. Any operation signed using private session key with
attached session token is treated as performed by the original client.

## Types

NeoFS CLI supports two ways to execute operation within a session depending on
whether the user of the command application is an original user (1) or a trusted
one (2).

### Dynamic

For case (1) CLI user can only open dynamic sessions. Protocol call
`SessionService.Create` is used for this purpose. As a result of the call, a
private session key will be generated on the server, thus making the remote
server trusted. This type of session is useful when the client needs to
transfer part of the responsibility for the formation of strict system elements
to the trusted server. At the moment, the approach is applicable only to
creating objects.

```shell
$ neofs-cli session create --rpc-endpoint <server_ip> --out ./blank_token
```
After this example command remote node holds session private key while its
public part is written into the session token encoded into the output file.
Later this token can be attached to the operations which support dynamic
sessions. Then the token will be finally formed and signed by CLI itself.

### Static

For case (2) CLI user can act on behalf of the person who issued the session
token to him. Unlike (1) the token must be fully prepared on the side of the
original client, and the CLI uses it only for reading. Ready token MUST have:
- correct context (object, container, etc.)
- valid lifetime
- public session key corresponding to the CLI key
- valid client signature

To sign the session token, exec:
```shell
$ neofs-cli --wallet <client_wallet> util sign session-token --from ./blank_token --to ./token
```
Once the token is signed, it MUST NOT be modified.

## Commands

### Object

Here are sub-commands of `object` command which support only dynamic sessions (1):
- `put`
- `delete`
- `lock`

These commands accept blank token of the dynamically opened session or open
session internally if it has not been opened yet.

All other `object` sub-commands support only static sessions (2).

### Container

List of commands supporting sessions (static only):
- `create`
- `delete`
- `set-eacl`
