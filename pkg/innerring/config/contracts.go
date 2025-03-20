package config

type Contracts struct {
	NeoFS      string   `mapstructure:"neofs"`
	Processing string   `mapstructure:"processing"`
	Audit      string   `mapstructure:"audit"`
	Balance    string   `mapstructure:"balance"`
	Container  string   `mapstructure:"container"`
	Netmap     string   `mapstructure:"netmap"`
	Proxy      string   `mapstructure:"proxy"`
	Reputation string   `mapstructure:"reputation"`
	Alphabet   Alphabet `mapstructure:"alphabet"`
}

type Alphabet struct {
	Az      string `mapstructure:"az"`
	Buky    string `mapstructure:"buky"`
	Vedi    string `mapstructure:"vedi"`
	Glagoli string `mapstructure:"glagoli"`
	Dobro   string `mapstructure:"dobro"`
	Yest    string `mapstructure:"yest"`
	Zhivete string `mapstructure:"zhivete"`
}
