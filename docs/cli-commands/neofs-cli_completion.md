## neofs-cli completion

Generate completion script

### Synopsis

To load completions:

Bash:
  $ source <(neofs-cli completion bash)

  To load completions for each session, execute once:
  Linux:
  $ neofs-cli completion bash > /etc/bash_completion.d/neofs-cli
  MacOS:
  $ neofs-cli completion bash > /usr/local/etc/bash_completion.d/neofs-cli

Zsh:
  If shell completion is not already enabled in your environment you will need
  to enable it.  You can execute the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  To load completions for each session, execute once:
  $ neofs-cli completion zsh > "${fpath[1]}/_neofs-cli"

  You will need to start a new shell for this setup to take effect.

Fish:
  $ neofs-cli completion fish | source

  To load completions for each session, execute once:
  $ neofs-cli completion fish > ~/.config/fish/completions/neofs-cli.fish


```
neofs-cli completion [bash|zsh|fish|powershell]
```

### Options

```
  -h, --help   help for completion
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli](neofs-cli.md)	 - Command Line Tool to work with NeoFS

