## neofs-cli object nodes

Show nodes for an object

### Synopsis

Show nodes taking part in an object placement at the current epoch.

```
neofs-cli object nodes [flags]
```

### Options

```
      --cid string            Container ID.
  -h, --help                  help for nodes
      --oid string            Object ID.
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --short                 Short node output info
  -t, --timeout duration      Timeout for the operation (default 15s)
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli object](neofs-cli_object.md)	 - Operations with Objects

