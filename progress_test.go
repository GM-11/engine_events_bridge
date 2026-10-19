package main

import (
	"reflect"
	"testing"
)

func TestFromSeqIsSentPlusOne(t *testing.T) {
	p := NewProgress([]uint32{1, 2}, map[uint32]uint64{1: 10})
	if got, want := p.FromSeq(), (map[uint32]uint64{1: 11, 2: 1}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	p.MarkSent(2, 5)
	if p.FromSeq()[2] != 6 {
		t.Fatalf("after MarkSent: %v", p.FromSeq())
	}
}

func TestUnknownCheckpointSymbolsDropped(t *testing.T) {
	p := NewProgress([]uint32{1}, map[uint32]uint64{1: 4, 9: 100})
	if got, want := p.AckedCopy(), (map[uint32]uint64{1: 4}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if _, ok := p.FromSeq()[9]; ok {
		t.Fatal("symbol 9 should be dropped")
	}
}

func TestMarkAckedKeepsMax(t *testing.T) {
	p := NewProgress([]uint32{1}, nil)
	p.MarkAcked(1, 7)
	p.MarkAcked(1, 3)
	if p.AckedCopy()[1] != 7 {
		t.Fatalf("got %v", p.AckedCopy())
	}
	if p.Sent(1) != 0 {
		t.Fatal("acking must not move sent")
	}
}
