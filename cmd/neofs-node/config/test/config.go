package configtest

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

func fromFile(path string) *config.Config {
	os.Clearenv() // ENVs have priority over config files, so we do this in tests

	cfg, err := config.New(config.WithConfigFile(path))
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func fromEnvFile(path string) *config.Config {
	loadEnv(path) // github.com/joho/godotenv can do that as well

	cfg, err := config.New()
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func forEachFile(paths []string, f func(*config.Config)) {
	for i := range paths {
		f(fromFile(paths[i]))
	}
}

// ForEachFileType passes configs read from next files:
//   - `<pref>.yaml`;
//   - `<pref>.json`.
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
	os.Clearenv()

	cfg, err := config.New()
	if err != nil {
		log.Fatal(err)
	}
	return cfg
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
