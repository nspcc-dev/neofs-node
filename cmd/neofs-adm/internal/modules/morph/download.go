package morph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/go-github/v39/github"
	"github.com/spf13/cobra"
)

func downloadContractsFromGithub(cmd *cobra.Command) (io.ReadCloser, error) {
	gcl := github.NewClient(nil)
	release, _, err := gcl.Repositories.GetLatestRelease(context.Background(), "nspcc-dev", "neofs-contract")
	if err != nil {
		return nil, fmt.Errorf("can't fetch release info: %w", err)
	}

	cmd.Printf("Found %s (%s), downloading...\n", release.GetTagName(), release.GetName())

	var url string
	for _, a := range release.Assets {
		if strings.HasPrefix(a.GetName(), "neofs-contract") {
			url = a.GetBrowserDownloadURL()
			break
		}
	}
	if url == "" {
		return nil, errors.New("can't find contracts archive in release assets")
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("can't fetch contracts archive: %w", err)
	}
	return resp.Body, nil
}
