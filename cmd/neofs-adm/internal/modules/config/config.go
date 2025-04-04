package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type configTemplate struct {
	Endpoint                string
	AlphabetDir             string
	MaxObjectSize           int
	EpochDuration           int
	BasicIncomeRate         int
	AuditFee                int
	CandidateFee            int
	ContainerFee            int
	ContainerAliasFee       int
	WithdrawFee             int
	AlphabetNames           []string
	HomomorphicHashDisabled bool
}

const configTxtTemplate = `rpc-endpoint: {{ .Endpoint}}
alphabet-wallets: {{ .AlphabetDir}}
network:
  max_object_size: {{ .MaxObjectSize}}
  epoch_duration: {{ .EpochDuration}}
  basic_income_rate: {{ .BasicIncomeRate}}
  homomorphic_hash_disabled: {{ .HomomorphicHashDisabled}}
  fee:
    audit: {{ .AuditFee}}
    candidate: {{ .CandidateFee}}
    container: {{ .ContainerFee}}
    container_alias: {{ .ContainerAliasFee }}
    withdraw: {{ .WithdrawFee}}
# if credentials section is omitted, then neofs-adm will require manual password input
credentials:{{ range.AlphabetNames}}
  {{.}}: password{{end}}
`

func initConfig(cmd *cobra.Command, _ []string) error {
	configPath, err := readConfigPathFromArgs(cmd)
	if err != nil {
		return nil
	}

	pathDir := filepath.Dir(configPath)
	err = os.MkdirAll(pathDir, 0o700)
	if err != nil {
		return fmt.Errorf("create dir %s: %w", pathDir, err)
	}

	f, err := os.OpenFile(configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", configPath, err)
	}
	defer f.Close()

	configText, err := generateConfigExample(pathDir, 7)
	if err != nil {
		return err
	}

	_, err = f.WriteString(configText)
	if err != nil {
		return fmt.Errorf("writing to %s: %w", configPath, err)
	}

	cmd.Printf("Initial config file saved to %s\n", configPath)

	return nil
}

func readConfigPathFromArgs(cmd *cobra.Command) (string, error) {
	configPath, err := cmd.Flags().GetString(configPathFlag)
	if err != nil {
		return "", err
	}

	if configPath != "" {
		return configPath, nil
	}

	return defaultConfigPath()
}

func defaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home dir path: %w", err)
	}

	return filepath.Join(home, ".neofs", "adm", "config.yml"), nil
}

// generateConfigExample builds .yml representation of the config file. It is
// easier to build it manually with template instead of using viper, because we
// want to order records in specific order in file and, probably, provide
// some comments as well.
func generateConfigExample(appDir string, credSize int) (string, error) {
	tmpl := configTemplate{
		Endpoint:                "https://neo.rpc.node:30333",
		MaxObjectSize:           67108864,      // 64 MiB
		EpochDuration:           240,           // 1 hour with 15s per block
		BasicIncomeRate:         1_0000_0000,   // 0.0001 GAS per GiB (Fixed12)
		HomomorphicHashDisabled: false,         // object homomorphic hash is enabled
		AuditFee:                1_0000,        // 0.00000001 GAS per audit (Fixed12)
		CandidateFee:            100_0000_0000, // 100.0 GAS (Fixed8)
		ContainerFee:            1000,          // 0.000000001 * 7 GAS per container (Fixed12)
		ContainerAliasFee:       500,           // ContainerFee / 2
		WithdrawFee:             1_0000_0000,   // 1.0 GAS (Fixed8)
		AlphabetNames:           make([]string, 0, credSize),
	}

	appDir, err := filepath.Abs(appDir)
	if err != nil {
		return "", fmt.Errorf("making absolute path for %s: %w", appDir, err)
	}
	tmpl.AlphabetDir = filepath.Join(appDir, "alphabet-wallets")

	for i := range credSize {
		tmpl.AlphabetNames = append(tmpl.AlphabetNames, client.NNSAlphabetContractName(i))
	}

	t, err := template.New("config.yml").Parse(configTxtTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing config template: %w", err)
	}

	buf := bytes.NewBuffer(nil)

	err = t.Execute(buf, tmpl)
	if err != nil {
		return "", fmt.Errorf("generating config from template: %w", err)
	}

	return buf.String(), nil
}

func GetPassword(v *viper.Viper, name string) (string, error) {
	key := "credentials." + name
	if v.IsSet(key) {
		return v.GetString(key), nil
	}

	prompt := "Password for " + name + " wallet > "
	return input.ReadPassword(prompt)
}

func GetStoragePassword(v *viper.Viper, name string) (string, error) {
	key := "storage." + name
	if name != "" && v.IsSet(key) {
		return v.GetString(key), nil
	}
	return input.ReadPassword("New password > ")
}
