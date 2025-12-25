## neofs-cli object lock

Lock object in container

### Synopsis

Lock object in container

```
neofs-cli object lock [flags]
```

### Options

```
      --address string                 Address of wallet account
      --bearer string                  File with signed JSON or binary encoded bearer token
      --cid string                     Container ID.
  -e, --expire-at uint                 The last active epoch for the lock
  -g, --generate-key                   Generate new private key
  -h, --help                           help for lock
      --lifetime uint                  Lock lifetime
      --oid strings                    Object ID.
  -r, --rpc-endpoint string            Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string                 Filepath to a JSON- or binary-encoded token of the object PUT session
      --session-subjects strings       Session subject user IDs (optional, defaults to current node)
      --session-subjects-nns strings   Session subject NNS names (optional, defaults to current node)
  -t, --timeout duration               Timeout for the operation (default 15s)
      --ttl uint32                     TTL value in request meta header (default 2)
  -w, --wallet string                  Path to the wallet
  -x, --xhdr strings                   Request X-Headers in form of Key=Value
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli object](neofs-cli_object.md)	 - Operations with Objects

