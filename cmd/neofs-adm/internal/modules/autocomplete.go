package modules

import (
	"os"

	"github.com/spf13/cobra"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

$ source <(neofs-adm completion bash)

# To load completions for each session, execute once:
Linux:
  $ neofs-adm completion bash > /etc/bash_completion.d/neofs-adm
MacOS:
  $ neofs-adm completion bash > /usr/local/etc/bash_completion.d/neofs-adm

Zsh:

# If shell completion is not already enabled in your environment you will need
# to enable it.  You can execute the following once:

$ echo "autoload -U compinit; compinit" >> ~/.zshrc

# To load completions for each session, execute once:
$ neofs-adm completion zsh > "${fpath[1]}/_neofs-adm"

# You will need to start a new shell for this setup to take effect.

Fish:

$ neofs-adm completion fish | source

# To load completions for each session, execute once:
$ neofs-adm completion fish > ~/.config/fish/completions/neofs-adm.fish
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			_ = cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			_ = cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			_ = cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			_ = cmd.Root().GenPowerShellCompletion(os.Stdout)
		}
	},
}
