package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	blobstorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

const (
	pathFlag   = "path"
	configFlag = "config"
)

func execute(args []string, out, errOut io.Writer) error {
	var path, cfgPath string

	fs := flag.NewFlagSet("fstree-decompress", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.StringVar(&path, pathFlag, "", "Path to FSTree root")
	fs.StringVar(&cfgPath, configFlag, "", "Path to storage node YAML configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if path == "" && cfgPath == "" {
		return fmt.Errorf("either %q or %q flag must be set", pathFlag, configFlag)
	}
	if path != "" && cfgPath != "" {
		return fmt.Errorf("%q and %q flags are mutually exclusive", pathFlag, configFlag)
	}
	if path != "" {
		return runPath(out, path)
	}
	return runConfig(out, cfgPath)
}

func runPath(out io.Writer, path string) error {
	if err := validateFSTreeRoot(path); err != nil {
		return err
	}

	fst := fstree.New(
		fstree.WithPath(path),
		fstree.WithPerm(0o600),
		fstree.WithSubtype(fstree.SubtypeBlobstor),
	)
	return runTree(out, fst)
}

func validateFSTreeRoot(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat FSTree root %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("FSTree root %q is not a directory", path)
	}

	descriptor := filepath.Join(path, ".fstree.json")
	info, err = os.Stat(descriptor)
	if err != nil {
		return fmt.Errorf("stat FSTree descriptor %q: %w", descriptor, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("FSTree descriptor %q is not a regular file", descriptor)
	}
	return nil
}

func runConfig(out io.Writer, cfgPath string) error {
	cfg, err := nodeconfig.New(nodeconfig.WithConfigFile(cfgPath))
	if err != nil {
		return fmt.Errorf("read node config: %w", err)
	}

	for i := range cfg.Storage.ShardList {
		sh := cfg.Storage.ShardList[i]
		if sh.Mode == mode.Disabled {
			continue
		}
		if sh.Blobstor.Type != fstree.Type {
			fmt.Fprintf(out, "shard %d (%s): skipped unsupported blobstor type %s\n", i, sh.Blobstor.Path, sh.Blobstor.Type)
			continue
		}

		fmt.Fprintf(out, "shard %d (%s):\n", i, sh.Blobstor.Path)
		if err = runTree(out, newFSTreeFromConfig(sh.Blobstor)); err != nil {
			return fmt.Errorf("process shard %d: %w", i, err)
		}
	}
	return nil
}

func newFSTreeFromConfig(cfg blobstorconfig.Blobstor) *fstree.FSTree {
	return fstree.New(
		fstree.WithPath(cfg.Path),
		fstree.WithPerm(cfg.Perm),
		fstree.WithDepth(cfg.Depth),
		fstree.WithNoSync(*cfg.NoSync),
		fstree.WithSubtype(fstree.SubtypeBlobstor),
		fstree.WithCombinedCountLimit(cfg.CombinedCountLimit),
		fstree.WithCombinedSizeLimit(int(cfg.CombinedSizeLimit)),
		fstree.WithCombinedSizeThreshold(int(cfg.CombinedSizeThreshold)),
		fstree.WithCombinedWriteInterval(cfg.FlushInterval),
	)
}

func runTree(out io.Writer, fst *fstree.FSTree) error {
	if err := fst.Open(false); err != nil {
		return fmt.Errorf("open FSTree: %w", err)
	}
	defer func() { _ = fst.Close() }()

	if err := fst.Init(common.ID{}); err != nil {
		return fmt.Errorf("init FSTree: %w", err)
	}

	stats, err := fst.RewriteCompressed()
	printStats(out, stats)
	return err
}

func printStats(out io.Writer, st fstree.RewriteCompressedStats) {
	fmt.Fprintf(out, "scanned: %d\n", st.Scanned)
	fmt.Fprintf(out, "compressed: %d\n", st.Compressed)
	fmt.Fprintf(out, "rewritten: %d\n", st.Rewritten)
	fmt.Fprintf(out, "skipped: %d\n", st.Skipped)
	fmt.Fprintf(out, "failed: %d\n", st.Failed)
	fmt.Fprintf(out, "compressed_bytes: %d\n", st.CompressedBytes)
	fmt.Fprintf(out, "plain_bytes: %d\n", st.PlainBytes)
}

func main() {
	cmderr.ExitOnErr(execute(os.Args[1:], os.Stdout, os.Stderr))
}
