## neofs-cli bearer create

Create bearer token

### Synopsis

Create bearer token.

All epoch flags can be specified relative to the current epoch with the +n syntax.
In this case --rpc-endpoint flag should be specified and the epoch in bearer token
is set to current epoch + n.


```
neofs-cli bearer create [flags]
```

### Options

```
  -e, --eacl string               Path to the extended ACL table
  -x, --expire-at string          The last active epoch for the token
  -h, --help                      help for create
  -i, --issued-at string          Epoch to issue token at
      --json                      Output token in JSON
  -l, --lifetime uint             Number of epochs for token to stay valid
  -n, --not-valid-before string   Not valid before epoch
      --out string                File to write token to
  -o, --owner string              Token owner
  -r, --rpc-endpoint string       Remote node address (as 'multiaddr' or '<host>:<port>')
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli bearer](neofs-cli_bearer.md)	 - Operations with bearer token

