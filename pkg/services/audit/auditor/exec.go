package auditor

import (
	"fmt"

	"go.uber.org/zap"
)

// Execute audits container data.
func (c *Context) Execute() {
	c.init()

	checks := []struct {
		name string
		exec func()
	}{
		{name: "PoR", exec: c.executePoR},
		{name: "PoP", exec: c.executePoP},
		{name: "PDP", exec: c.executePDP},
	}

	for i := range checks {
		c.log.Debug(fmt.Sprintf("executing %s check...", checks[i].name))

		if c.expired() {
			break
		}

		checks[i].exec()

		if i == len(checks)-1 {
			c.complete()
		}
	}

	c.writeReport()
}

func (c *Context) executePDP() {
	// TODO: replace logging with real algorithm
	log := c.log.With(zap.Int("nodes in container", c.cnrNodesNum))

	for i := range c.pairs {
		log.Debug("next pair for hash game",
			zap.String("node 1", c.pairs[i].n1.Address()),
			zap.String("node 2", c.pairs[i].n2.Address()),
			zap.Stringer("object", c.pairs[i].id),
		)
	}
}
