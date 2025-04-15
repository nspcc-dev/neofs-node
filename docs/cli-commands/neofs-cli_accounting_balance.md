## neofs-cli accounting balance

Get internal balance of NeoFS account

### Synopsis

Get internal balance of NeoFS account

```
neofs-cli accounting balance [flags]
```

### Options

```
      --address string        Address of wallet account
  -g, --generate-key          Generate new private key
  -h, --help                  help for balance
      --owner string          owner of balance account (omit to use owner from private key)
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -t, --timeout duration      Timeout for the operation (default 15s)
  -w, --wallet string         Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli accounting](neofs-cli_accounting.md)	 - Operations with accounts and balances

