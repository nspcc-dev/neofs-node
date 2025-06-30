## neofs-cli session create

Create session token

### Synopsis

Create session token.

Default lifetime of session token is 10 epochs
if none of --expire-at or --lifetime flags is specified.


```
neofs-cli session create [flags]
```

### Options

```
      --address string        Address of wallet account
  -e, --expire-at uint        The last active epoch for token to stay valid
  -h, --help                  help for create
      --json                  Output token in JSON
  -l, --lifetime uint         Number of epochs for token to stay valid (default 10)
      --out string            File to write session token to
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -w, --wallet string         Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli session](neofs-cli_session.md)	 - Operations with session token

