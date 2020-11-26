package blobovnicza

// Blobovnicza represents the implementation of NeoFS Blobovnicza.
type Blobovnicza struct {
	*cfg
}

// Option is an option of Blobovnicza's constructor.
type Option func(*cfg)

type cfg struct {
}

func defaultCfg() *cfg {
	return &cfg{}
}

// New creates and returns new Blobovnicza instance.
func New(opts ...Option) *Blobovnicza {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Blobovnicza{
		cfg: c,
	}
}
