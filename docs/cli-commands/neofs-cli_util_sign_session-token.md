## neofs-cli util sign session-token

Sign session token to use it in requests

```
neofs-cli util sign session-token [flags]
```

### Options

```
      --address string   Address of wallet account
      --force-issuer     Set configured account as the session issuer even if it is already specified
      --from string      File with JSON encoded session token to sign
  -g, --generate-key     Generate new private key
  -h, --help             help for session-token
      --to string        File to save signed session token (optional)
  -w, --wallet string    Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli util sign](neofs-cli_util_sign.md)	 - Sign NeoFS structure

