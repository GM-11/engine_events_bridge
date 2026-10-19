package main

import (
	"errors"
	"io"
	"testing"

	pb "engine_events_bridge/gen/exchange/v1"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type pending struct {
	rec  *kgo.Record
	done func(*kgo.Record, error)
}

type harness struct {
	prog     *Progress
	produced []pending
	fatals   []error
}

func newHarness() *harness { return &harness{prog: NewProgress([]uint32{1, 2}, nil)} }

func (h *harness) produce(rec *kgo.Record, done func(*kgo.Record, error)) {
	h.produced = append(h.produced, pending{rec, done})
}
func (h *harness) fatal(err error) { h.fatals = append(h.fatals, err) }

func (h *harness) run(msgs ...*pb.FeedMessage) (int, error) {
	i := 0
	recv := func() (*pb.FeedMessage, error) {
		if i == len(msgs) {
			return nil, io.EOF
		}
		i++
		return msgs[i-1], nil
	}
	return readLoop(recv, h.produce, h.prog, h.fatal)
}

func ev(symbol uint32, seq uint64) *pb.FeedMessage {
	return &pb.FeedMessage{Body: &pb.FeedMessage_Event{Event: &pb.MarketEvent{Symbol: symbol, Seq: seq}}}
}

func TestReadLoopProducesInOrder(t *testing.T) {
	h := newHarness()
	got, err := h.run(ev(1, 1), ev(1, 2), ev(1, 3))
	if !errors.Is(err, io.EOF) || got != 3 || len(h.produced) != 3 {
		t.Fatalf("got=%d err=%v produced=%d", got, err, len(h.produced))
	}
	for i, p := range h.produced {
		if string(p.rec.Key) != "1" {
			t.Fatalf("key %q", p.rec.Key)
		}
		var out pb.MarketEvent
		if err := proto.Unmarshal(p.rec.Value, &out); err != nil {
			t.Fatal(err) // value must be MarketEvent bytes
		}
		if out.GetSeq() != uint64(i+1) || out.GetSymbol() != 1 {
			t.Fatalf("record %d: %v", i, &out)
		}
	}
	if h.prog.Sent(1) != 3 {
		t.Fatalf("sent %d", h.prog.Sent(1))
	}
}

func TestReadLoopSkipsDuplicates(t *testing.T) {
	h := newHarness()
	h.run(ev(1, 1), ev(1, 2), ev(1, 2), ev(1, 3))
	if len(h.produced) != 3 {
		t.Fatalf("produced %d, want 3", len(h.produced))
	}
}

func TestReadLoopGapIsAnError(t *testing.T) {
	h := newHarness()
	_, err := h.run(ev(1, 1), ev(1, 3))
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("want gap error, got %v", err)
	}
	if len(h.produced) != 1 || h.prog.Sent(1) != 1 {
		t.Fatalf("produced %d sent %d", len(h.produced), h.prog.Sent(1))
	}
}

func TestReadLoopSymbolsAreIndependent(t *testing.T) {
	h := newHarness()
	h.run(ev(1, 1), ev(2, 1), ev(2, 2), ev(1, 2))
	if len(h.produced) != 4 {
		t.Fatalf("produced %d", len(h.produced))
	}
}

func TestReadLoopSnapshotIsFatal(t *testing.T) {
	h := newHarness()
	snap := &pb.FeedMessage{Body: &pb.FeedMessage_Snapshot{Snapshot: &pb.BookSnapshot{Symbol: 1}}}
	h.run(snap, ev(1, 1))
	if len(h.fatals) != 1 || len(h.produced) != 0 {
		t.Fatalf("fatals %d produced %d", len(h.fatals), len(h.produced))
	}
}

func TestProduceErrorIsFatalAndNotAcked(t *testing.T) {
	h := newHarness()
	h.run(ev(1, 1))
	h.produced[0].done(h.produced[0].rec, errors.New("broker down"))
	if len(h.fatals) != 1 || h.prog.AckedCopy()[1] != 0 {
		t.Fatalf("fatals %d acked %v", len(h.fatals), h.prog.AckedCopy())
	}
}

func TestAckAdvancesOnlyOnSuccess(t *testing.T) {
	h := newHarness()
	h.run(ev(1, 1), ev(1, 2))
	if h.prog.AckedCopy()[1] != 0 {
		t.Fatal("acked before Kafka confirmed")
	}
	h.produced[0].done(h.produced[0].rec, nil)
	if h.prog.AckedCopy()[1] != 1 || h.prog.Sent(1) != 2 {
		t.Fatalf("acked %v sent %d", h.prog.AckedCopy(), h.prog.Sent(1))
	}
	h.produced[1].done(h.produced[1].rec, nil)
	if h.prog.AckedCopy()[1] != 2 || len(h.fatals) != 0 {
		t.Fatalf("acked %v fatals %d", h.prog.AckedCopy(), len(h.fatals))
	}
}
