package config_test

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

func fromFile(path string) *config.Config {
	var p config.Prm

	return config.New(p,
		config.WithConfigFile(path),
	)
}

func forEachFile(paths []string, f func(*config.Config)) {
	for i := range paths {
		f(fromFile(paths[i]))
	}
}

func forEachFileType(pref string, f func(*config.Config)) {
	forEachFile([]string{
		pref + ".yaml",
		pref + ".json",
	}, f)
}
