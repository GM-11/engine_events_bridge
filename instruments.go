package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadInstruments returns the symbol ids from the shared instruments file.
func LoadInstruments(path string) ([]uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read instruments: %w", err)
	}
	var file struct {
		Instruments []struct {
			ID uint32 `json:"id"`
		} `json:"instruments"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse instruments %s: %w", path, err)
	}
	if len(file.Instruments) == 0 {
		return nil, fmt.Errorf("instruments %s: no instruments", path)
	}
	seen := make(map[uint32]bool, len(file.Instruments))
	ids := make([]uint32, 0, len(file.Instruments))
	for _, in := range file.Instruments {
		if seen[in.ID] {
			return nil, fmt.Errorf("instruments %s: duplicate id %d", path, in.ID)
		}
		seen[in.ID] = true
		ids = append(ids, in.ID)
	}
	return ids, nil
}
