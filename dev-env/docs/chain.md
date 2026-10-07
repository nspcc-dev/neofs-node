# N3 main chain privnet service

A single-node N3 privnet deployment, running on
[neo-go](https://github.com/nspcc-dev/neo-go). Represents N3 MainNet.

Contracts deployed:
- NeoFS [contract](https://github.com/nspcc-dev/neofs-contract/tree/master/contracts/neofs)

RPC available at `http://main-chain.neofs.devenv:30333`.

## .env settings

### NEOGO_VERSION

Version of neo-go docker container for main chain deployment.

## Main chain wallets

There is a wallet with GAS that used for contract deployment: 
`wallets/wallet.json`. This wallet has one account with **empty password**.

```
$ neo-go wallet nep17 balance \
    -w wallets/wallet.json \
    -r http://main-chain.neofs.devenv:30333 
   
Account NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM
GAS: GasToken (d2a4cff31913016155e38e474a2c06d08be276cf)
        Amount : 9978.0074623
        Updated: 34
```

If you want to operate in main chain with your personal wallet (e.g. to make 
a deposit in NeoFS contract), you can transfer GAS from there.

1. Create new wallet.

```
$ neo-go wallet init -a -w wallets/neofs1.json

Enter the name of the account > neofs1
Enter passphrase >
Confirm passphrase >

{
    "version": "3.0",
    "accounts": [
        {
            "address": "NXnzw3J9VvKXjM1BPAJK4QUpTtEQu4TpU6",
...
wallet successfully created, file location is wallets/neofs1.json
```

2. Transfer GAS from `wallets/wallet.json`. The password is empty.

```
$ neo-go wallet nep17 transfer \
    -w wallets/wallet.json \
    -r http://main-chain.neofs.devenv:30333 \
    --from NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
    --to NXnzw3J9VvKXjM1BPAJK4QUpTtEQu4TpU6 \
    --amount 50 \
    --token GAS
```

3. Check it's there.

```
$ neo-go wallet nep17 balance \
    -w wallets/neofs1.json \
    -r http://main-chain.neofs.devenv:30333

Account NXnzw3J9VvKXjM1BPAJK4QUpTtEQu4TpU6 
GAS: GasToken (d2a4cff31913016155e38e474a2c06d08be276cf)
        Amount : 50
        Updated: 14689
```

## Claim GAS from consensus node

If there is no enough GAS on `wallets/wallet.json` account, you can claim some
GAS to consensus node's wallet and then transfer it.

Consensus node is running with `services/chain/node-wallet.json` wallet. It has
multiple accounts with the password `one`.


Claim GAS to consensus node's wallet. Use account that contains NEO tokens. 
```
$ neo-go wallet claim \
    -w services/chain/node-wallet.json \
    -r http://main-chain.neofs.devenv:30333 \
    -a NPpKskku5gC6g59f2gVRR8fmvUTLDp9w7Y \
Password >
70e09bbd55846dcc7cee23905b737c63e5a80d32e387bce108bc6db8e641fb90
```

Then you can transfer GAS the same way as it was done in previous section.

```
$ neo-go wallet nep17 transfer \
    -w services/chain/node-wallet.json \
    -r http://main-chain.neofs.devenv:30333 \
    --from NPpKskku5gC6g59f2gVRR8fmvUTLDp9w7Y \
    --to NXnzw3J9VvKXjM1BPAJK4QUpTtEQu4TpU6 \
    --amount 50 \
    --token GAS
```

## NeoFS GAS deposit

NeoFS identifies users by their Neo wallet key pair. To start using NeoFS in
devenv you need to transfer some GAS to NeoFS contract in main chain.

Invoke `bin/deposit.sh` script by running `make prepare.ir` command to transfer
50 GAS from account in `wallets/wallet.json` file. Script enters passwords
automatically with `expect` utility.

```
$ make prepare.ir 
Password > 
Can't find matching token in the wallet. Querying RPC-node for balances.
6713c776f4102300691d9c3c493bcd3402434f5e32e8147e0a5bc72209a1e410
```

Script converts addresses and executes this command:
```
$ neo-go wallet nep17 transfer \
    -w wallets/wallet.json \
    -r http://main-chain.neofs.devenv:30333 \
    --from NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
    --to NerhjaqJsJt4LxMqUbkkVMpsF2d9TtcpFv \
    --token GAS \
    --amount 50
```

You can specify any wallet address scripthash in the transfer's data argument,
and NeoFS deposit will be transferred to that address.

```
$ neo-go wallet nep17 transfer \
    -w wallets/wallet.json \
    -r http://main-chain.neofs.devenv:30333 \
    --from NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
    --to NerhjaqJsJt4LxMqUbkkVMpsF2d9TtcpFv \
    --token GAS \
    --amount 50 \
    hash160:bd711de066e9c2f7b502c7f3f0e0a6f1c8341edd
```
