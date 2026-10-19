package main

import (
	"os"
	"strings"
)

type Config struct {
	EngineFeed      string
	KafkaBrokers    []string
	Topic           string
	CheckpointPath  string
	GatewayID       string
	InstrumentsPath string
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func LoadConfig() Config {
	var brokers []string
	for _, b := range strings.Split(env("BRIDGE_KAFKA_BROKERS", "localhost:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return Config{
		EngineFeed:      env("BRIDGE_ENGINE_FEED", "127.0.0.1:50061"),
		KafkaBrokers:    brokers,
		Topic:           env("BRIDGE_TOPIC", "engine.events.v1"),
		CheckpointPath:  env("BRIDGE_CHECKPOINT", "./data/checkpoint.json"),
		GatewayID:       env("BRIDGE_GATEWAY_ID", "bridge"),
		InstrumentsPath: env("EXCHANGE_INSTRUMENTS", "../config/instruments.json"),
	}
}
