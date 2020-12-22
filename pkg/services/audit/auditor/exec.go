package auditor

import (
	"fmt"
)

// Execute audits container data.
func (c *Context) Execute() {
	c.init()

	for _, check := range []struct {
		name string
		exec func()
	}{
		{name: "PoR", exec: c.executePoR},
		{name: "PoP", exec: c.executePoP},
		{name: "PDP", exec: c.executePDP},
	} {
		c.log.Debug(fmt.Sprintf("executing %s check...", check.name))

		if c.expired() {
			break
		}

		check.exec()
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
