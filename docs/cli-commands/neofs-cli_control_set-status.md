## neofs-cli control set-status

Set status of the storage node in NeoFS network map

### Synopsis

Set status of the storage node in NeoFS network map

```
neofs-cli control set-status [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for set-status
      --status string      New netmap status keyword ('online', 'offline', 'maintenance')
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

