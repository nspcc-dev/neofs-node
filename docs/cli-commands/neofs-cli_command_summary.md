# NeoFS CLI Command Overview

This document summarizes every command available under `docs/cli-commands` so you can quickly navigate the NeoFS CLI surface. Each entry includes its purpose, the most important flags, and a sample invocation. Refer to `neofs-cli --help` or `<command> --help` for the authoritative flag list.

## 1. CLI Basics

- Root command: `neofs-cli [flags]`
- Common global flags: `-c/--config` (custom config file), `-v/--verbose`, `--version`, `-h/--help`
- Most subcommands accept contextual flags such as `-r/--rpc-endpoint`, `-w/--wallet`, `--address`, and `--session` to target specific networks, accounts, or sessions.

## 2. accounting: balances

| Command | Purpose | Key flags | Example |
| --- | --- | --- | --- |
| `neofs-cli accounting balance` | Show the balance of a NeoFS account | `--address`, `--wallet` | `neofs-cli accounting balance --wallet wallet.json --address NSxxx` |

## 3. acl: access control

### 3.1 basic ACL

| Command | Purpose | Notes |
| --- | --- | --- |
| `neofs-cli acl basic print` | Display predefined basic ACL presets (`private`, `public-read`, etc.) | Useful when choosing `--basic-acl` during container creation |

### 3.2 extended ACL

| Command | Purpose | Key flags | Example |
| --- | --- | --- | --- |
| `neofs-cli acl extended create` | Build an eACL table from inline or file-based rules | `--cid`, `--rule/-r`, `--file/-f`, `--out/-o` | `neofs-cli acl extended create --cid <CID> -f rules.txt -o table.json` |
| `neofs-cli acl extended print` | Print an existing eACL table in readable form | `--table`, `--json` | `neofs-cli acl extended print --table table.json` |

## 4. bearer: bearer tokens

| Command | Purpose | Example |
| --- | --- | --- |
| `neofs-cli bearer create` | Create a bearer token from ACL rules | `neofs-cli bearer create --cid <CID> --file rules.txt --out bearer.json` |
| `neofs-cli bearer print` | Decode and display a bearer token | `neofs-cli bearer print --token bearer.json` |

## 5. completion: shell completion

- `neofs-cli completion <shell>`: generate completion scripts for `bash`, `zsh`, `fish`, etc.

## 6. container: container management

| Command | Focus | Key flags | Example |
| --- | --- | --- | --- |
| `neofs-cli container create` | Create and register a container | `--wallet`, `--policy/-p`, `--basic-acl`, `--attributes`, `--session` | `neofs-cli container create --wallet wallet.json --policy policy.json --basic-acl public-read` |
| `neofs-cli container delete` | Remove a container | `--cid`, `--session`, `--force` | `neofs-cli container delete --cid <CID> --wallet wallet.json` |
| `neofs-cli container get` | Fetch container metadata | `--cid`, `--json`, `--to` | `neofs-cli container get --cid <CID> --json` |
| `neofs-cli container list` | List containers owned by an account | `--wallet` or `--address` | `neofs-cli container list --wallet wallet.json` |
| `neofs-cli container list-objects` | Enumerate objects in a container | `--cid` plus optional filters | `neofs-cli container list-objects --cid <CID>` |
| `neofs-cli container get-eacl` | Download current eACL table | `--cid`, `--json`, `--to` | `neofs-cli container get-eacl --cid <CID> --json --to eacl.json` |
| `neofs-cli container set-eacl` | Upload a new eACL table | `--cid`, `--table`, `--session`, `--force` | `neofs-cli container set-eacl --cid <CID> --table eacl.json --wallet wallet.json` |
| `neofs-cli container nodes` | Show storage nodes serving the container | `--cid` | `neofs-cli container nodes --cid <CID>` |

## 7. control: storage node control

### 7.1 General commands

| Command | Purpose | Example |
| --- | --- | --- |
| `neofs-cli control healthcheck` | Probe storage node health | `neofs-cli control healthcheck --rpc-endpoint <host:port>` |
| `neofs-cli control drop-objects` | Force-delete objects | `neofs-cli control drop-objects --cid <CID> --oid <OID>` |
| `neofs-cli control set-status` | Change node service mode | `neofs-cli control set-status --status maintenance` |

### 7.2 Notary helpers

| Command | Purpose |
| --- | --- |
| `neofs-cli control notary list` | List pending notary requests |
| `neofs-cli control notary request` | Submit a notary request (e.g., container ops) |
| `neofs-cli control notary sign` | Sign and approve a notary request |

### 7.3 Object helpers

| Command | Purpose |
| --- | --- |
| `neofs-cli control object list` | List objects known to the node |
| `neofs-cli control object status` | Inspect object state |
| `neofs-cli control object revive` | Restore soft-deleted objects |

### 7.4 Shard management

| Command | Purpose |
| --- | --- |
| `neofs-cli control shards list` | Enumerate all shards |
| `neofs-cli control shards set-mode` | Switch shard modes (rw, ro, etc.) |
| `neofs-cli control shards evacuate` | Evacuate shard data |
| `neofs-cli control shards restore` | Re-import a shard |
| `neofs-cli control shards dump` | Dump shard metadata |
| `neofs-cli control shards flush-cache` | Clear shard cache |

## 8. gendoc: auto-generated docs

- `neofs-cli gendoc --dir <path>` scans the current command tree and emits Markdown (the files in this directory were produced this way).

## 9. netmap: network topology

| Command | Purpose | Example |
| --- | --- | --- |
| `neofs-cli netmap epoch` | Show current epoch | `neofs-cli netmap epoch --rpc-endpoint <host:port>` |
| `neofs-cli netmap nodeinfo` | Describe a specific node | `neofs-cli netmap nodeinfo --node <pubkey>` |
| `neofs-cli netmap netinfo` | Dump global network info | `neofs-cli netmap netinfo` |
| `neofs-cli netmap snapshot` | Export a netmap snapshot | `neofs-cli netmap snapshot --out netmap.json` |

## 10. object: object service

| Command | Purpose | Typical use |
| --- | --- | --- |
| `neofs-cli object put` | Upload an object | Combine with `--attributes`, `--file`, `--cid` |
| `neofs-cli object get` | Download an object | `--cid`, `--oid`, `--file` |
| `neofs-cli object head` | Inspect object metadata | `--cid`, `--oid`, `--json` |
| `neofs-cli object delete` | Delete an object | Optional `--tombstone` |
| `neofs-cli object range` | Download partial data by offset | `--offset`, `--length` |
| `neofs-cli object hash` | Compute data hash | Integrity verification |
| `neofs-cli object lock` | Lock an object against deletion | Provide lock duration |
| `neofs-cli object search` / `searchv2` | Search objects via filters | `--cid`, `--filters`, `--limit` |
| `neofs-cli object nodes` | Show nodes storing the object | Useful for troubleshooting |

## 11. request: raw requests

| Command | Purpose |
| --- | --- |
| `neofs-cli request create-container` | Craft and send a raw container creation request (useful for debugging or notary flows) |

## 12. session: session tokens

| Command | Purpose | Example |
| --- | --- | --- |
| `neofs-cli session create` | Build PUT/DELETE/SETEACL session tokens | `neofs-cli session create --wallet wallet.json --verb put --cid <CID> --out put-session.json` |

## 13. util: helpers

| Subcommand | Purpose |
| --- | --- |
| `neofs-cli util convert` | Convert object/container descriptors (JSON ↔ binary) |
| `neofs-cli util convert eacl` | Convert eACL encodings |
| `neofs-cli util keyer` | Generate/inspect key pairs |
| `neofs-cli util sign` | Generic signer |
| `neofs-cli util sign bearer-token` | Sign bearer tokens |
| `neofs-cli util sign session-token` | Sign session tokens |

## 14. Practical tips

- Most commands honor networking flags such as `--timeout` and `--ttl` to control request lifetime.
- Use `--await` on long-running operations (e.g., `container set-eacl --await`) to wait for FS chain confirmation.
- Compare CLI and SDK compatibility quickly via `neofs-cli --version`.

> This summary complements, but does not replace, the official `--help` output. Always consult the command-specific help for the complete flag set and constraints.

