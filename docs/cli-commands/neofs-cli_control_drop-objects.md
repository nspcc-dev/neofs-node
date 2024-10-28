## neofs-cli control drop-objects

Drop objects from the node's local storage

### Synopsis

Drop objects from the node's local storage

```
neofs-cli control drop-objects [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for drop-objects
  -o, --objects strings    List of object addresses to be removed in string format
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

