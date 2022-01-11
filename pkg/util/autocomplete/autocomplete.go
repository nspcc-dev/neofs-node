package autocomplete

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const longHelpTemplate = `To load completions:

Bash:
  $ source <(%s completion bash)

  To load completions for each session, execute once:
  Linux:
  $ %s completion bash > /etc/bash_completion.d/%s
  MacOS:
  $ %s completion bash > /usr/local/etc/bash_completion.d/%s

Zsh:
  If shell completion is not already enabled in your environment you will need
  to enable it.  You can execute the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
  
  To load completions for each session, execute once:
  $ %s completion zsh > "${fpath[1]}/_%s"
  
  You will need to start a new shell for this setup to take effect.

Fish:
  $ %s completion fish | source
  
  To load completions for each session, execute once:
  $ %s completion fish > ~/.config/fish/completions/%s.fish
`

// Command returns cobra command structure for autocomplete routine.
func Command(name string) *cobra.Command {
	return &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate completion script",
		Long: fmt.Sprintf(longHelpTemplate,
			name, name, name, name, name, name, name, name, name, name),
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
}
