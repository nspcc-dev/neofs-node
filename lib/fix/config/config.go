package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Params groups the parameters of configuration.
type Params struct {
	File    string
	Type    string
	Prefix  string
	Name    string
	Version string

	AppDefaults func(v *viper.Viper)
}

// NewConfig is a configuration tool's constructor.
func NewConfig(p Params) (v *viper.Viper, err error) {
	v = viper.New()
	v.SetEnvPrefix(p.Prefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("app.name", p.Name)
	v.SetDefault("app.version", p.Version)

	if p.AppDefaults != nil {
		p.AppDefaults(v)
	}

	if p.fromFile() {
		v.SetConfigFile(p.File)
		v.SetConfigType(p.safeType())

		err = v.ReadInConfig()
	}

	return v, err
}

func (p Params) fromFile() bool {
	return p.File != ""
}

func (p Params) safeType() string {
	if p.Type == "" {
		p.Type = "yaml"
	}
	return strings.ToLower(p.Type)
}
