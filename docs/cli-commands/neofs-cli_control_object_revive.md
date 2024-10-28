## neofs-cli control object revive

Forcefully revive object

### Synopsis

Purge removal marks from metabases

```
neofs-cli control object revive [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for revive
      --object string      Object address
  -t, --timeout duration   Timeout for the operation (default 15s)
  -w, --wallet string      Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli control object](neofs-cli_control_object.md)	 - Direct object operations with storage engine

