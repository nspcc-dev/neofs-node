# NeoFS Quick Start Guide

Follow the steps below to upload and manage files on NeoFS using the Neo CLI and `neofs-cli` utilities.

## 1. Download the Neo CLI

Download the latest NeoGo or Neo CLI from the official release page and extract it locally.[^neo-cli]

```text
https://github.com/nspcc-dev/neo-go/releases
https://github.com/neo-project/neo/releases/
```

## 2. Create a Wallet

Launch the Neo CLI executable and create a wallet file:

```shell
./neo-go wallet init wallet.json
./neo-go wallet create -w wallet.json
```

Remember the password—you will need it whenever the wallet is accessed.

## 3. Fund the Wallet

Copy your newly generated N3 address from the CLI. Visit the Neo faucet and request testnet GAS (1 GAS is sufficient for this guide).[^faucet]

```text
https://neoxwish.ngd.network/
```

## 4. Download NeoFS CLI

Download the latest `neofs-cli` release (packaged with `neofs-node`) and unzip it into a working directory.[^neofs-cli]

```text
https://github.com/nspcc-dev/neofs-node/releases
```

## 5. Provide the Wallet File

Copy `wallet.json` into the root directory of the extracted `neofs-cli` bundle so every command can reference it with a relative path.

## 6. Deposit GAS to the NeoFS Contract

Use a node or Neo wallet (for example, neo-go or neoline) to deposit **1 GAS** on N3 testnet into the NeoFS contract address:

```text
NZAUkYbJ1Cb2HrNmwZ1pg9xYHBhm2FgtKV
```
Command can be:
```shell
./neo-go wallet nep17 transfer --token GAS --amount 1 --to NZAUkYbJ1Cb2HrNmwZ1pg9xYHBhm2FgtKV -r grpcs://st4.t5.fs.neo.org:8082
```
Wait for the transaction to confirm on-chain.

## 7. Check NeoFS Balance

Verify that your NeoFS balance reflects the deposit:

```shell
./neofs-cli accounting balance \
  -r grpcs://st4.t5.fs.neo.org:8082 \
  --owner <YOUR_N3_ADDRESS>
```

Replace `<YOUR_N3_ADDRESS>` with the address from step 2 (e.g., `NfrMnAzQUB9N5kaz1F1ZUkqLgECMpxewtv`).

## 8. Create a Public Container

Create a NeoFS container with public read/write ACLs:

```shell
./neofs-cli -r grpcs://st4.t5.fs.neo.org:8082 \
  --wallet wallet.json \
  container create \
  --policy 'REP 3' \
  --basic-acl eacl-public-read-write \
  --await
```

Save the returned **Container ID**; you will need it to upload and delete objects.

## 9. Upload an Object

Upload a file (example: `NEO.jpg`) into the container:

```shell
./neofs-cli \
  --rpc-endpoint grpcs://st4.t5.fs.neo.org:8082 \
  -w wallet.json \
  object put \
  --cid <CONTAINER_ID> \
  --file NEO.jpg
```

Record the **Object ID** from the command output.

## 10. Preview the File

Access the file through a public gateway, substituting your container and object IDs:

```text
https://rest.t5.fs.neo.org/v1/objects/<CONTAINER_ID>/by_id/<OBJECT_ID>
```

Example:

```text
https://rest.fs.neo.org/v1/objects/CeeroywT8ppGE4HGjhpzocJkdb2yu3wD5qCGFTjkw1Cc/by_id/45cmFDEAnB9TEgp6Gh433HHCP1NvK4hXQhzAh2to3EaJ
```

## 11. Delete the Object

Remove the uploaded file when you no longer need it:

```shell
./neofs-cli \
  --wallet wallet.json \
  -r grpcs://st4.t5.fs.neo.org:8082 \
  object delete \
  --cid <CONTAINER_ID> \
  --oid <OBJECT_ID>
```

The object will be marked for deletion according to NeoFS garbage-collection policies.
