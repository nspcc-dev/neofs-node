package auditor

import (
	"fmt"
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

func (c *Context) executePoR() {
	// TODO: implement me
}

func (c *Context) executePoP() {
	// TODO: implement me
}

func (c *Context) executePDP() {
	// TODO: implement me
}
