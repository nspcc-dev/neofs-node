package configtest

import (
	"bufio"
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

func fromFile(path string) *config.Config {
	var p config.Prm

	os.Clearenv() // ENVs have priority over config files so we do this in tests

	return config.New(p,
		config.WithConfigFile(path),
	)
}

func fromEnvFile(path string) *config.Config {
	var p config.Prm

	loadEnv(path) // github.com/joho/godotenv can do that as well

	return config.New(p)
}

func forEachFile(paths []string, f func(*config.Config)) {
	for i := range paths {
		f(fromFile(paths[i]))
	}
}

// ForEachFileType passes configs read from next files:
//  - `<pref>.yaml`;
//  - `<pref>.json`.
func ForEachFileType(pref string, f func(*config.Config)) {
	forEachFile([]string{
		pref + ".yaml",
		pref + ".json",
	}, f)
}

// ForEnvFileType creates config from `<pref>.env` file.
func ForEnvFileType(pref string, f func(*config.Config)) {
	f(fromEnvFile(pref + ".env"))
}

// EmptyConfig returns config without any values and sections.
func EmptyConfig() *config.Config {
	var p config.Prm

	return config.New(p)
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
		pair := strings.SplitN(scanner.Text(), "=", 2)
		if len(pair) != 2 {
			continue
		}

		pair[1] = strings.Trim(pair[1], `"`)

		err = os.Setenv(pair[0], pair[1])
		if err != nil {
			panic("can't set environment variable")
		}
	}
}
