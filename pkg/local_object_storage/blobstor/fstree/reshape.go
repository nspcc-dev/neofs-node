package fstree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var (
	reshapeProgressStep  = 10_000
	reshapeRetryInterval = time.Minute
)

func (t *FSTree) startReshape() {
	if t.secondaryDepth == 0 || t.secondaryDepth == t.Depth {
		return
	}

	t.reshapeStateMtx.Lock()
	defer t.reshapeStateMtx.Unlock()
	if t.reshapeCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.reshapeCancel = cancel
	t.reshapeDone = done

	go func() {
		defer func() {
			t.reshapeStateMtx.Lock()
			t.reshapeCancel = nil
			t.reshapeStateMtx.Unlock()
			close(done)
		}()

		t.log.Info("FSTree reshaping started",
			zap.Uint64("old_depth", t.secondaryDepth),
			zap.Uint64("new_depth", t.Depth),
			zap.String("path", t.RootPath),
		)

		t.runReshape(ctx)
	}()
}

func (t *FSTree) runReshape(ctx context.Context) {
	var movedTotal int
	for {
		moved, err := t.reshape(ctx)
		movedTotal += moved
		if errors.Is(err, context.Canceled) {
			t.log.Info("FSTree reshaping stopped",
				zap.Uint64("old_depth", t.secondaryDepth),
				zap.Uint64("new_depth", t.Depth),
				zap.Int("moved_files", movedTotal),
				zap.String("path", t.RootPath),
			)
			return
		}
		if err == nil {
			err = t.completeReshape()
			if err == nil {
				t.log.Info("FSTree reshaping completed",
					zap.Uint64("old_depth", t.secondaryDepth),
					zap.Uint64("new_depth", t.Depth),
					zap.Int("moved_files", movedTotal),
					zap.String("path", t.RootPath),
				)
				return
			}
		}

		t.log.Warn("FSTree reshaping failed, will retry",
			zap.Uint64("old_depth", t.secondaryDepth),
			zap.Uint64("new_depth", t.Depth),
			zap.Int("moved_files", movedTotal),
			zap.String("path", t.RootPath),
			zap.Duration("retry_after", reshapeRetryInterval),
			zap.Error(err),
		)
		timer := time.NewTimer(reshapeRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (t *FSTree) stopReshape() {
	t.reshapeStateMtx.Lock()
	cancel, done := t.reshapeCancel, t.reshapeDone
	t.reshapeStateMtx.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (t *FSTree) reshape(ctx context.Context) (int, error) {
	lastProcessedPath, err := t.reshapeLastProcessedPath()
	if err != nil {
		return 0, err
	}

	var moved int
	var dirty bool
	if err := t.reshapeDir(ctx, t.RootPath, 0, "", lastProcessedPath, &moved, &dirty); err != nil {
		return moved, err
	}
	remaining, err := t.hasSecondaryFiles(ctx, t.RootPath, 0, "")
	if err != nil {
		return moved, err
	}
	if remaining {
		return moved, errors.New("secondary layout still contains object files")
	}
	return moved, nil
}

func (t *FSTree) hasSecondaryFiles(ctx context.Context, dir string, depth uint64, prefix string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("read directory %q: %w", dir, err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if depth < t.secondaryDepth {
			if !entry.IsDir() {
				continue
			}
			found, err := t.hasSecondaryFiles(ctx, filepath.Join(dir, entry.Name()), depth+1, prefix+entry.Name())
			if err != nil || found {
				return found, err
			}
			continue
		}
		if !entry.IsDir() {
			if _, err := addressFromString(prefix + entry.Name()); err == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func (t *FSTree) reshapeDir(ctx context.Context, dir string, depth uint64, prefix, lastProcessedPath string, moved *int, dirty *bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read directory %q: %w", dir, err)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		path := filepath.Join(dir, entry.Name())
		if depth < t.secondaryDepth {
			if !entry.IsDir() {
				continue
			}
			if err := t.reshapeDir(ctx, path, depth+1, prefix+entry.Name(), lastProcessedPath, moved, dirty); err != nil {
				return err
			}
			continue
		}

		if entry.IsDir() {
			continue
		}
		addr, err := addressFromString(prefix + entry.Name())
		if err != nil {
			continue
		}
		relativePath, err := filepath.Rel(t.RootPath, path)
		if err != nil {
			return fmt.Errorf("make path %q relative to FSTree root: %w", path, err)
		}
		relativePath = filepath.ToSlash(relativePath)
		if lastProcessedPath != "" && relativePath <= lastProcessedPath {
			continue
		}
		movedFile, err := t.reshapeFile(path, *addr, dirty)
		if err != nil {
			return err
		}
		if movedFile {
			*moved = *moved + 1
			if *moved%reshapeProgressStep == 0 {
				if err := t.updateReshapeProgress(relativePath); err != nil {
					return err
				}
				*dirty = false
				t.log.Info("FSTree reshaping progress",
					zap.Uint64("old_depth", t.secondaryDepth),
					zap.Uint64("new_depth", t.Depth),
					zap.Int("moved_files", *moved),
					zap.String("path", t.RootPath),
				)
			}
		}
	}

	if t.secondaryDepth > t.Depth && depth > t.Depth {
		if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("remove empty directory %q: %w", dir, err)
		}
	}

	return nil
}

func (t *FSTree) reshapeFile(oldPath string, addr oid.Address, dirty *bool) (bool, error) {
	newPath := t.treePath(addr)
	if oldPath == newPath {
		return false, nil
	}

	if t.Depth > t.secondaryDepth {
		if err := util.MkdirAllX(filepath.Dir(newPath), t.Permissions); err != nil {
			return false, fmt.Errorf("create destination directory for %q: %w", newPath, err)
		}
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("rename file %q to %q: %w", oldPath, newPath, err)
	}
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("remove old file %q: %w", oldPath, err)
	}
	*dirty = true
	return true, nil
}

func (t *FSTree) syncReshape(dirty bool) error {
	if t.noSync || !dirty {
		return nil
	}
	if err := syncFS(t.RootPath); err != nil {
		return fmt.Errorf("sync filesystem for reshape: %w", err)
	}
	return nil
}
