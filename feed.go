package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	pb "engine_events_bridge/gen/exchange/v1"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Simple counters for the periodic stats line.
var (
	statProduced   atomic.Uint64
	statAcked      atomic.Uint64
	statReconnects atomic.Uint64
)

type recvFunc func() (*pb.FeedMessage, error)
type produceFunc func(rec *kgo.Record, done func(*kgo.Record, error))

// Run keeps one engine feed stream open and copies its events to Kafka until
// ctx is done or a fatal error is reported.
//
// produceCtx is NOT ctx: franz-go fails every buffered record whose context is
// cancelled, so tying records to ctx would fail them on Ctrl-C (or on a stream
// reconnect, for a stream ctx). main cancels produceCtx only after the flush
// deadline on shutdown.
func Run(ctx, produceCtx context.Context, feed pb.EngineFeedClient, cl *kgo.Client, prog *Progress,
	gatewayID string, fatal func(error)) {
	produce := func(rec *kgo.Record, done func(*kgo.Record, error)) { cl.Produce(produceCtx, rec, done) }

	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		req := &pb.FeedSubscribeRequest{GatewayId: gatewayID, FromSeq: prog.FromSeq()} // no symbols = all
		sctx, cancel := context.WithCancel(ctx)
		var got int
		stream, err := feed.Subscribe(sctx, req)
		if err == nil {
			got, err = readLoop(stream.Recv, produce, prog, fatal)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		switch status.Code(err) {
		case codes.OutOfRange:
			fatal(fmt.Errorf("events lost: %s (engine replay window too small, or engine/sequencer was reset)",
				status.Convert(err).Message()))
			return
		case codes.InvalidArgument, codes.NotFound:
			fatal(fmt.Errorf("config mismatch with engine: %s", status.Convert(err).Message()))
			return
		}
		slog.Warn("engine feed ended, reconnecting", "err", err, "events_read", got, "backoff", backoff)
		statReconnects.Add(1)
		if got > 0 {
			backoff = 200 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// readLoop copies events from one stream to Kafka and returns how many it read.
// It returns when the stream fails, a gap is seen, or a fatal condition is hit.
func readLoop(recv recvFunc, produce produceFunc, prog *Progress, fatal func(error)) (int, error) {
	got := 0
	for {
		msg, err := recv()
		if err != nil {
			return got, err
		}
		if msg.GetSnapshot() != nil {
			fatal(fmt.Errorf("engine sent a snapshot to a from_seq subscriber: engine lacks replay support"))
			return got, nil
		}
		ev := msg.GetEvent()
		if ev == nil {
			continue
		}
		s, seq := ev.GetSymbol(), ev.GetSeq()
		want := prog.Sent(s) + 1
		if seq < want {
			continue // already sent
		}
		if seq > want {
			return got, fmt.Errorf("gap on symbol %d: want %d got %d", s, want, seq)
		}
		value, err := proto.Marshal(ev) // MarketEvent bytes (contract §5), not FeedMessage
		if err != nil {
			fatal(fmt.Errorf("marshal symbol %d seq %d: %w", s, seq, err))
			return got, nil
		}
		rec := &kgo.Record{Key: []byte(strconv.FormatUint(uint64(s), 10)), Value: value}
		produce(rec, func(_ *kgo.Record, err error) {
			if err != nil {
				fatal(fmt.Errorf("kafka produce symbol %d seq %d: %w", s, seq, err))
				return
			}
			prog.MarkAcked(s, seq)
			statAcked.Add(1)
		})
		prog.MarkSent(s, seq)
		statProduced.Add(1)
		got++
	}
}
