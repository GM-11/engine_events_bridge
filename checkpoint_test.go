package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "checkpoint.json")
	want := map[uint32]uint64{1: 1203, 2: 88}
	if err := SaveCheckpoint(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp left behind: %v", err)
	}
}

func TestCheckpointMissingFileIsEmpty(t *testing.T) {
	got, err := LoadCheckpoint(filepath.Join(t.TempDir(), "none.json"))
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestCheckpointBadFiles(t *testing.T) {
	for name, content := range map[string]string{
		"corrupt":     `{"version":1,`,
		"bad version": `{"version":2,"last_acked":{}}`,
		"bad key":     `{"version":1,"last_acked":{"x":1}}`,
	} {
		path := filepath.Join(t.TempDir(), "c.json")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCheckpoint(path); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
