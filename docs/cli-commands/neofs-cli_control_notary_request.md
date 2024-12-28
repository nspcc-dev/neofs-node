## neofs-cli control notary request

Create and send a notary request

### Synopsis

Create and send a notary request with one of the following methods:
- newEpoch, transaction for creating of new NeoFS epoch event in FS chain, no args
- setConfig, transaction to add/update global config value in the NeoFS network, 1 arg in the form key=val
- removeNode, transaction to move nodes to the Offline state in the candidates list, 1 arg is the public key of the node

```
neofs-cli control notary request [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for request
      --method string      Requested method
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

