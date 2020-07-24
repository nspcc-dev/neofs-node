package test

import (
	"testing"
)

func TestNewStorage(t *testing.T) {
	s := New()

	Storage(t, s)
}
