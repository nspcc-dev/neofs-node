## neofs-cli control healthcheck

Health check of the NeoFS node

### Synopsis

Health check of the NeoFS node. Checks storage node by default, use --ir flag to work with Inner Ring.

```
neofs-cli control healthcheck [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for healthcheck
      --ir                 Communicate with IR node
  -t, --timeout duration   Timeout for the operation (default 15s)
  -w, --wallet string      Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli control](neofs-cli_control.md)	 - Operations with storage node

