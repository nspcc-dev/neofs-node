package configtest

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/stretchr/testify/require"
)

func fromFile(t *testing.T, path string) *config.Config {
	os.Clearenv() // ENVs have priority over config files, so we do this in tests

	return newConfig(t, config.WithConfigFile(path))
}

func fromEnvFile(t *testing.T, path string) *config.Config {
	loadEnv(path) // github.com/joho/godotenv can do that as well

	return newConfig(t)
}

func forEachFile(t *testing.T, paths []string, f func(*config.Config)) {
	for i := range paths {
		f(fromFile(t, paths[i]))
	}
}

// ForEachFileType passes configs read from next files:
//   - `<pref>.yaml`;
//   - `<pref>.json`.
func ForEachFileType(t *testing.T, pref string, f func(*config.Config)) {
	forEachFile(t, []string{
		pref + ".yaml",
		pref + ".json",
	}, f)
}

// ForEnvFileType creates config from `<pref>.env` file.
func ForEnvFileType(t *testing.T, pref string, f func(*config.Config)) {
	f(fromEnvFile(t, pref+".env"))
}

// EmptyConfig returns config without any values and sections.
func EmptyConfig(t *testing.T) *config.Config {
	os.Clearenv()

	return newConfig(t)
}

// loadEnv reads .env file, parses `X=Y` records and sets OS ENVs.
func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		panic("can't open .env file")
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		envVar, value, found := strings.Cut(scanner.Text(), "=")
		if !found {
			continue
		}

		value = strings.Trim(value, `"`)

		err = os.Setenv(envVar, value)
		if err != nil {
			panic("can't set environment variable")
		}
	}
}

func newConfig(t *testing.T, opts ...config.Option) *config.Config {
	cfg, err := config.New(opts...)
	require.NoError(t, err)
	return cfg
}
