package main

import (
	"log/slog"
	"maps"
	"sync"
)

// Progress tracks, per symbol, what was handed to the Kafka client (sent) and
// what Kafka confirmed (acked). Reconnects use sent+1; the checkpoint uses acked.
type Progress struct {
	mu    sync.Mutex
	sent  map[uint32]uint64 // highest seq handed to the Kafka client (this process only)
	acked map[uint32]uint64 // highest seq Kafka confirmed
}

func NewProgress(symbols []uint32, checkpoint map[uint32]uint64) *Progress {
	p := &Progress{sent: make(map[uint32]uint64, len(symbols)), acked: make(map[uint32]uint64, len(symbols))}
	for _, s := range symbols {
		p.sent[s] = checkpoint[s]
		p.acked[s] = checkpoint[s]
	}
	for s := range checkpoint {
		if _, ok := p.sent[s]; !ok {
			slog.Warn("checkpoint symbol not in instruments, dropped", "symbol", s)
		}
	}
	return p
}

func (p *Progress) Sent(sym uint32) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent[sym]
}

func (p *Progress) MarkSent(sym uint32, seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq > p.sent[sym] {
		p.sent[sym] = seq
	}
}

func (p *Progress) MarkAcked(sym uint32, seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq > p.acked[sym] {
		p.acked[sym] = seq
	}
}

func (p *Progress) AckedCopy() map[uint32]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[uint32]uint64, len(p.acked))
	maps.Copy(out, p.acked)
	return out
}

func (p *Progress) FromSeq() map[uint32]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[uint32]uint64, len(p.sent))
	for s, seq := range p.sent {
		out[s] = seq + 1
	}
	return out
}
