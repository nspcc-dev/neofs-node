package morph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/google/go-github/v39/github"
	"github.com/spf13/cobra"
)

func downloadContractsFromGithub(cmd *cobra.Command) (string, error) {
	gcl := github.NewClient(nil)
	release, _, err := gcl.Repositories.GetLatestRelease(context.Background(), "nspcc-dev", "neofs-contract")
	if err != nil {
		return "", fmt.Errorf("can't fetch release info: %w", err)
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
		return "", errors.New("can't find contracts archive in release assets")
	}

	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("can't fetch contracts archive: %w", err)
	}
	defer resp.Body.Close()

	f, err := os.CreateTemp("", "neofs-contract-*.tar.gz")
	if err != nil {
		return "", fmt.Errorf("can't allocate temporary file: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return f.Name(), err
}
