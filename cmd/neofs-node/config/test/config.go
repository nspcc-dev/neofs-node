package configtest

import "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"

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

// ForEachFileType passes configs read from next files:
//  - `<pref>.yaml`;
//  - `<pref>.json`.
func ForEachFileType(pref string, f func(*config.Config)) {
	forEachFile([]string{
		pref + ".yaml",
		pref + ".json",
	}, f)
}

// EmptyConfig returns config without any values and sections.
func EmptyConfig() *config.Config {
	var p config.Prm

	return config.New(p)
}
