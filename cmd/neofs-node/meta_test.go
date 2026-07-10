package main

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config"
)

func TestMetadataChainCompatibility(t *testing.T) {
	if config.HFLatestStable != config.HFGorgon {
		t.Fatalf("new unknown stable hardfork (%s), adapt it", config.HFLatestStable)
	}

	hfs := metadataChainHardforks()
	for _, hf := range config.StableHardforks {
		if _, ok := hfs[hf.String()]; !ok {
			t.Fatalf("%s stable hardfork is not included in metadata chain default start, adopt it", hf.String())
		}
	}
}
