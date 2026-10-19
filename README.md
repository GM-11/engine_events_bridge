# Engine Events Bridge

Copies the engine's public feed (`EngineFeed.Subscribe`) into Kafka topic `engine.events.v1`,
so the cash owner can settle trades from Kafka. The engine never waits on Kafka: this process
is the only thing that does.

- Subscribes with `from_seq = last sent + 1` per symbol, so the engine replays what was missed
  (see `ENGINE_REPLAY_WINDOW` on the engine) and never sends a snapshot.
- Produces each `MarketEvent` (protobuf bytes) with key = symbol id, so one symbol stays on one partition.
- Keeps the last **Kafka-confirmed** seq per symbol in a checkpoint file (written every second and on shutdown).
- Delivery into Kafka is at-least-once: after a crash some events may appear twice. Readers dedupe by `seq` per symbol.
- No gaps: a seq jump on the stream makes it reconnect from the missing seq; events the engine no longer
  retains (`OUT_OF_RANGE`) are fatal (exit 1).

## Environment

| Env | Default |
|---|---|
| `BRIDGE_ENGINE_FEED` | `127.0.0.1:50061` |
| `BRIDGE_KAFKA_BROKERS` | `localhost:9092` (comma-separated) |
| `BRIDGE_TOPIC` | `engine.events.v1` |
| `BRIDGE_CHECKPOINT` | `./data/checkpoint.json` |
| `BRIDGE_GATEWAY_ID` | `bridge` |
| `EXCHANGE_INSTRUMENTS` | `../config/instruments.json` |

## Run

Start order: Kafka (`infra/kafka`, topic must exist) → sequencer → engine → bridge → cash owner → gateway.

```
go run .
```

## Develop

```
./gen.sh            # regenerate gen/ from ../proto (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
go vet ./... && go test -race ./...
```

## Known limits / dev reset

There is no sequencer journal yet: restarting the **sequencer** restarts all numbering at 1, and the
bridge then waits forever for its old number. Dev reset: stop the bridge, delete `data/checkpoint.json`,
delete and recreate the topic.
