package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

const checkpointVersion = 1

type checkpointFile struct {
	Version   int               `json:"version"`
	LastAcked map[string]uint64 `json:"last_acked"`
}

// LoadCheckpoint returns the last Kafka-confirmed seq per symbol. A missing file
// is a first run (empty map); anything unreadable is an error, never a silent
// restart from 1.
func LoadCheckpoint(path string) (map[uint32]uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[uint32]uint64{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var file checkpointFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse checkpoint %s: %w", path, err)
	}
	if file.Version != checkpointVersion {
		return nil, fmt.Errorf("checkpoint %s: unsupported version %d", path, file.Version)
	}
	acked := make(map[uint32]uint64, len(file.LastAcked))
	for key, seq := range file.LastAcked {
		sym, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("checkpoint %s: bad symbol key %q", path, key)
		}
		acked[uint32(sym)] = seq
	}
	return acked, nil
}

// SaveCheckpoint writes the file atomically: temp file, fsync, rename.
func SaveCheckpoint(path string, acked map[uint32]uint64) error {
	file := checkpointFile{Version: checkpointVersion, LastAcked: make(map[string]uint64, len(acked))}
	for sym, seq := range acked {
		file.LastAcked[strconv.FormatUint(uint64(sym), 10)] = seq
	}
	data, err := json.Marshal(file)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
