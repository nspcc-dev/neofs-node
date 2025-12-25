## neofs-cli object put

Put object to NeoFS

### Synopsis

Put object to NeoFS

```
neofs-cli object put [flags]
```

### Options

```
      --address string                 Address of wallet account
      --attributes strings             User attributes in form of Key1=Value1,Key2=Value2
      --bearer string                  File with signed JSON or binary encoded bearer token
      --binary                         Deserialize object structure from given file.
      --cid string                     Container ID.
      --disable-filename               Do not set well-known filename attribute
      --disable-timestamp              Do not set well-known timestamp attribute
  -e, --expire-at uint                 The last active epoch in the life of the object
      --file string                    File with object payload
  -g, --generate-key                   Generate new private key
  -h, --help                           help for put
  -l, --lifetime uint                  Number of epochs for object to stay valid
      --no-progress                    Do not show progress bar
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

