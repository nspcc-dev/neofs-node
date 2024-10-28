## neofs-cli control synchronize-tree

Synchronize log for the tree

### Synopsis

Synchronize log for the tree in an object tree service.

```
neofs-cli control synchronize-tree [flags]
```

### Options

```
      --address string     Address of wallet account
      --cid string         Container ID.
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
      --height uint        Starting height
  -h, --help               help for synchronize-tree
  -t, --timeout duration   Timeout for the operation (default 15s)
      --tree-id string     Tree ID
  -w, --wallet string      Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli control](neofs-cli_control.md)	 - Operations with storage node

