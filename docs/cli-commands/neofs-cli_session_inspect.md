## neofs-cli session inspect

Inspect session token (V1 or V2)

### Synopsis

Inspect and display information about a session token.

Supports both V1 (session.Object) and V2 (session.TokenV2) tokens.
Automatically detects the token version and displays relevant information.

Example usage:
  neofs-cli session inspect --token token.json
  neofs-cli session inspect --token token.bin


```
neofs-cli session inspect [flags]
```

### Options

```
  -h, --help           help for inspect
      --token string   Path to session token file (JSON or binary)
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli session](neofs-cli_session.md)	 - Operations with session token

