package config

// Contracts configures addresses of contracts in mainchain.
type Contracts struct {
	NeoFS      string `mapstructure:"neofs"`
	Processing string `mapstructure:"processing"`
}
