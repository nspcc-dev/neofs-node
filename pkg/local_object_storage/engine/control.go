package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// Open opens all StorageEngine's components.
func (e *StorageEngine) Open() error {
	return e.open()
}

func (e *StorageEngine) open() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	for id, sh := range e.shards {
		if err := sh.Open(); err != nil {
			if !e.cfg.isIgnoreUninitedShards {
				return fmt.Errorf("open shard %s: %w", id, err)
			}
			e.log.Debug("could not open shard",
				zap.String("id", id),
				zap.Error(err),
			)
			delete(e.shards, id)
		}
	}

	return nil
}

// Init initializes all StorageEngine's components.
func (e *StorageEngine) Init() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	for id, sh := range e.shards {
		if err := sh.Init(); err != nil {
			if !e.cfg.isIgnoreUninitedShards {
				return fmt.Errorf("init shard %s: %w", id, err)
			}
			e.log.Debug("could not init shard",
				zap.String("id", id),
				zap.Error(err),
			)
			delete(e.shards, id)
		}
	}

	err := e.deleteNotFoundContainers()
	if err != nil {
		return fmt.Errorf("obsolete containers cleanup: %w", err)
	}

	e.wg.Add(1)
	go e.setModeLoop()

	return nil
}

var errClosed = errors.New("storage engine is closed")

// Close releases all StorageEngine's components. Waits for all data-related operations to complete.
// After the call, all the next ones will fail.
//
// The method MUST only be called when the application exits.
func (e *StorageEngine) Close() error {
	close(e.closeCh)
	defer e.wg.Wait()
	return e.setBlockExecErr(errClosed)
}

// closes all shards. Never returns an error, shard errors are logged.
func (e *StorageEngine) close(releasePools bool) error {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	if releasePools {
		for _, p := range e.shardPools {
			p.Release()
		}
	}

	for id, sh := range e.shards {
		if err := sh.Close(); err != nil {
			e.log.Debug("could not close shard",
				zap.String("id", id),
				zap.Error(err),
			)
		}
	}

	return nil
}

// sets the flag of blocking execution of all data operations according to err:
//   - err != nil, then blocks the execution. If exec wasn't blocked, calls close method
//     (if err == errClosed => additionally releases pools and does not allow to resume executions).
//   - otherwise, resumes execution. If exec was blocked, calls open method.
//
// Can be called concurrently with exec. In this case it waits for all executions to complete.
func (e *StorageEngine) setBlockExecErr(err error) error {
	e.blockMtx.Lock()
	defer e.blockMtx.Unlock()

	prevErr := e.blockErr

	if errors.Is(prevErr, errClosed) {
		return errClosed
	}

	e.blockErr = err

	if err == nil {
		if prevErr != nil { // block -> ok
			return e.open()
		}
	} else if prevErr == nil { // ok -> block
		return e.close(errors.Is(err, errClosed))
	}

	// otherwise do nothing

	return nil
}

// BlockExecution blocks the execution of any data-related operation. All blocked ops will return err.
// To resume the execution, use ResumeExecution method.
//
// Сan be called regardless of the fact of the previous blocking. If execution wasn't blocked, releases all resources
// similar to Close. Can be called concurrently with Close and any data related method (waits for all executions
// to complete). Returns error if any Close has been called before.
//
// Must not be called concurrently with either Open or Init.
//
// Note: technically passing nil error will resume the execution, otherwise, it is recommended to call ResumeExecution
// for this.
func (e *StorageEngine) BlockExecution(err error) error {
	return e.setBlockExecErr(err)
}

// ResumeExecution resumes the execution of any data-related operation.
// To block the execution, use BlockExecution method.
//
// Сan be called regardless of the fact of the previous blocking. If execution was blocked, prepares all resources
// similar to Open. Can be called concurrently with Close and any data related method (waits for all executions
// to complete). Returns error if any Close has been called before.
//
// Must not be called concurrently with either Open or Init.
func (e *StorageEngine) ResumeExecution() error {
	return e.setBlockExecErr(nil)
}

type ReConfiguration struct {
	errorsThreshold uint32
	shardPoolSize   uint32

	shards map[string][]shard.Option // meta path -> shard opts
}

// SetErrorsThreshold sets a size amount of errors after which
// shard is moved to read-only mode.
func (rCfg *ReConfiguration) SetErrorsThreshold(errorsThreshold uint32) {
	rCfg.errorsThreshold = errorsThreshold
}

// SetShardPoolSize sets a size of worker pool for each shard.
func (rCfg *ReConfiguration) SetShardPoolSize(shardPoolSize uint32) {
	rCfg.shardPoolSize = shardPoolSize
}

// AddShard adds a shard for the reconfiguration.
// Shard identifier is calculated from paths used in blobstor.
func (rCfg *ReConfiguration) AddShard(id string, opts []shard.Option) {
	if rCfg.shards == nil {
		rCfg.shards = make(map[string][]shard.Option)
	}

	if _, found := rCfg.shards[id]; found {
		return
	}

	rCfg.shards[id] = opts
}

// Reload reloads StorageEngine's configuration in runtime.
func (e *StorageEngine) Reload(rcfg ReConfiguration) error {
	e.mtx.Lock()
	e.shardPoolSize = rcfg.shardPoolSize
	e.mtx.Unlock()

	type reloadInfo struct {
		sh   *shard.Shard
		opts []shard.Option
	}

	e.mtx.RLock()
	for _, pool := range e.shardPools {
		pool.Tune(int(e.shardPoolSize))
	}

	var shardsToRemove []string // shards IDs
	var shardsToAdd []string    // shard config identifiers (blobstor paths concatenation)
	var shardsToReload []reloadInfo

	// mark removed shards for removal
	for id, sh := range e.shards {
		_, ok := rcfg.shards[calculateShardID(sh.DumpInfo())]
		if !ok {
			shardsToRemove = append(shardsToRemove, id)
		}
	}

loop:
	for newID := range rcfg.shards {
		for _, sh := range e.shards {
			// This calculation should be kept in sync with node
			// configuration parsing during SIGHUP.
			if newID == calculateShardID(sh.DumpInfo()) {
				shardsToReload = append(shardsToReload, reloadInfo{
					sh:   sh.Shard,
					opts: rcfg.shards[newID],
				})
				continue loop
			}
		}

		shardsToAdd = append(shardsToAdd, newID)
	}

	e.mtx.RUnlock()

	e.removeShards(shardsToRemove...)

	for _, p := range shardsToReload {
		err := p.sh.Reload(p.opts...)
		if err != nil {
			e.log.Error("could not reload a shard",
				zap.Stringer("shard id", p.sh.ID()),
				zap.Error(err))
		}
	}

	for _, newID := range shardsToAdd {
		sh, err := e.createShard(rcfg.shards[newID])
		if err != nil {
			return fmt.Errorf("could not add new shard with '%s' metabase path: %w", newID, err)
		}

		idStr := sh.ID().String()

		err = sh.Open()
		if err == nil {
			err = sh.Init()
		}
		if err != nil {
			_ = sh.Close()
			return fmt.Errorf("could not init %s shard: %w", idStr, err)
		}

		err = e.addShard(sh)
		if err != nil {
			_ = sh.Close()
			return fmt.Errorf("could not add %s shard: %w", idStr, err)
		}

		e.log.Info("added new shard", zap.String("id", idStr))
	}

	return nil
}

func calculateShardID(info shard.Info) string {
	// This calculation should be kept in sync with node
	// configuration parsing during SIGHUP.
	var sb strings.Builder
	sb.WriteString(filepath.Clean(info.BlobStorInfo.Path))
	return sb.String()
}
