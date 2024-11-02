## neofs-cli util sign bearer-token

Sign bearer token to use it in requests

```
neofs-cli util sign bearer-token [flags]
```

### Options

```
      --address string   Address of wallet account
      --from string      File with JSON or binary encoded bearer token to sign
  -g, --generate-key     Generate new private key
  -h, --help             help for bearer-token
      --json             Dump bearer token in JSON encoding
      --to string        File to dump signed bearer token (default: binary encoded)
  -w, --wallet string    Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli util sign](neofs-cli_util_sign.md)	 - Sign NeoFS structure

