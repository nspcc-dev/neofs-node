package main

type cfg struct {
	grpcAddr string
}

func defaultCfg() *cfg {
	return &cfg{
		grpcAddr: ":50501",
	}
}
