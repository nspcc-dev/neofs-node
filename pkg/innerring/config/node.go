package config

type Node struct {
	PersistentState PersistentState `mapstructure:"persistent_state"`
}

type PersistentState struct {
	Path string `mapstructure:"path"`
}
