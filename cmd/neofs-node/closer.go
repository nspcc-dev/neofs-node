package main

type closer struct {
	name string
	fn   func()
}

func getCloser(c *cfg, name string) *closer {
	for _, clsr := range c.closers {
		if clsr.name == name {
			return &clsr
		}
	}
	return nil
}

func delCloser(c *cfg, name string) {
	for i, clsr := range c.closers {
		if clsr.name == name {
			c.closers[i] = c.closers[len(c.closers)-1]
			c.closers = c.closers[:len(c.closers)-1]
			return
		}
	}
}
