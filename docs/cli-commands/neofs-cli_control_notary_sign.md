## neofs-cli control notary sign

Sign notary request by its hash

### Synopsis

Sign notary request by its hash

```
neofs-cli control notary sign [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
      --hash string        hash of the notary request
  -h, --help               help for sign
  -t, --timeout duration   Timeout for the operation (default 15s)
  -w, --wallet string      Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli control notary](neofs-cli_control_notary.md)	 - Commands with notary request with alphabet key of inner ring node

