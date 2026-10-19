package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "engine_events_bridge/gen/exchange/v1"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() { os.Exit(run()) }

func run() int {
	cfg := LoadConfig()
	symbols, err := LoadInstruments(cfg.InstrumentsPath)
	if err != nil {
		slog.Error("setup failed", "err", err)
		return 1
	}
	checkpoint, err := LoadCheckpoint(cfg.CheckpointPath)
	if err != nil {
		slog.Error("setup failed", "err", err)
		return 1
	}
	prog := NewProgress(symbols, checkpoint)
	for _, s := range symbols {
		slog.Info("resume", "symbol", s, "resume_from", prog.Sent(s)+1)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedRecords(100_000), // full buffer → Produce blocks → bridge stops reading → engine drops it
		kgo.ProducerLinger(2*time.Millisecond),
	)
	if err != nil {
		slog.Error("kafka client failed", "err", err)
		return 1
	}
	if err := checkTopic(cl, cfg.Topic); err != nil {
		slog.Error("setup failed", "err", err)
		cl.Close()
		return 1
	}

	conn, err := grpc.NewClient(cfg.EngineFeed, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("engine feed client failed", "err", err)
		cl.Close()
		return 1
	}
	feed := pb.NewEngineFeedClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		once     sync.Once
		exitCode int
	)
	fatal := func(err error) {
		once.Do(func() {
			slog.Error("fatal", "err", err)
			exitCode = 1
			stop()
		})
	}

	var bg sync.WaitGroup
	bg.Add(2)
	go func() { // checkpoint saver
		defer bg.Done()
		saveLoop(ctx, cfg.CheckpointPath, prog)
	}()
	go func() { // stats
		defer bg.Done()
		statsLoop(ctx)
	}()

	slog.Info("bridge started", "engine_feed", cfg.EngineFeed, "topic", cfg.Topic, "symbols", len(symbols))
	Run(ctx, feed, cl, prog, cfg.GatewayID, fatal)
	bg.Wait()

	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := cl.Flush(flushCtx); err != nil {
		slog.Warn("flush did not finish", "err", err)
	}
	cancel()
	if err := SaveCheckpoint(cfg.CheckpointPath, prog.AckedCopy()); err != nil {
		slog.Error("final checkpoint save failed", "err", err)
	}
	cl.Close()
	conn.Close()
	once.Do(func() {}) // no late fatal can change the code now
	return exitCode
}

func checkTopic(cl *kgo.Client, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	topics, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("kafka unreachable: %w", err)
	}
	if d, ok := topics[topic]; !ok || d.Err != nil {
		return fmt.Errorf("topic %s missing: start infra/kafka (compose up)", topic)
	}
	return nil
}

// saveLoop writes the checkpoint every second when it changed.
func saveLoop(ctx context.Context, path string, prog *Progress) {
	var last map[uint32]uint64
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		cur := prog.AckedCopy()
		if maps.Equal(cur, last) {
			continue
		}
		if err := SaveCheckpoint(path, cur); err != nil {
			slog.Error("checkpoint save failed, will retry", "err", err)
			continue
		}
		last = cur
	}
}

func statsLoop(ctx context.Context) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			slog.Info("stats", "produced", statProduced.Load(), "acked", statAcked.Load(),
				"reconnects", statReconnects.Load())
		}
	}
}
