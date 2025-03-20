package config

// Node configures application state.
type Node struct {
	PersistentState PersistentState `mapstructure:"persistent_state"`
}

// PersistentState configures persistent state.
type PersistentState struct {
	Path string `mapstructure:"path"`
}
