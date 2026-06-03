package innerring

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config"
)

func TestMetadataChainCompatibility(t *testing.T) {
	if config.HFLatestStable != config.HFFaun {
		t.Fatalf("new unknown stable hardfork (%s), adapt it", config.HFLatestStable)
	}
}
