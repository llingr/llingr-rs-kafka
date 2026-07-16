// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// Example producer for the llingr-kafka end-to-end proof: a small, idiomatic
// franz-go producer that publishes a fixed run of order events to the "orders"
// topic, which the llingr-kafka consumer then processes. Between them they
// exercise the whole demux + franz + FFI chain against a real, authenticated
// broker (SASL/SCRAM-SHA-256 over TLS).
//
// It is a completely normal ecosystem Kafka client: franz-go's default
// key-based partitioner routes each keyed record, and ProduceSync awaits the
// broker's acknowledgement, which is acks=all because franz-go is an
// idempotent producer by default. The orderId is both the record key AND a
// body field, so the consumer can assert the key survived the round trip.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// order example message to publish into Kafka/RedPanda
type order struct {
	OrderID        string `json:"orderId"`
	CustomerID     string `json:"customerId"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int    `json:"unitPriceCents"`
	Currency       string `json:"currency"`
	PlacedAt       string `json:"placedAt"`
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}

// tlsConfig trusts the CA at caPath (the example's throwaway CA). No
// client certificate: SCRAM authenticates the client, TLS authenticates
// the broker and encrypts the transport.
func tlsConfig(caPath string) (*tls.Config, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("reading TLS CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", caPath)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	brokers := strings.Split(env("BROKERS", "redpanda:9092"), ",")
	topic := env("TOPIC", "orders")
	count := envInt("COUNT", 1000)

	// Security is env-driven and additive: with SASL_USERNAME set, the producer
	// authenticates with SCRAM-SHA-256 over TLS (sasl_ssl), trusting the CA at
	// TLS_CA_LOCATION. Without it, it connects in plaintext; the compose stack
	// always authenticates, but the binary is not coupled to one deployment.
	security := "plaintext"
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
	}
	if user := os.Getenv("SASL_USERNAME"); user != "" {
		pass := os.Getenv("SASL_PASSWORD")
		caPath := env("TLS_CA_LOCATION", "/certs/ca-cert.pem")
		tc, err := tlsConfig(caPath)
		if err != nil {
			log.Fatalf("TLS setup: %v", err)
		}
		opts = append(opts,
			kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()),
			kgo.DialTLSConfig(tc),
		)
		security = "sasl_ssl / SCRAM-SHA-256"
	}

	log.Printf("producing %d messages to topic %q via %s (%s)",
		count, topic, strings.Join(brokers, ","), security)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("creating kafka client: %v", err)
	}
	defer client.Close()

	customers := []string{"c-4711", "c-8100", "c-2049", "c-3312", "c-9930"}
	skus := []string{"SKU-0042", "SKU-1337", "SKU-2020", "SKU-7777", "SKU-0101"}

	records := make([]*kgo.Record, 0, count)
	for i := 0; i < count; i++ {
		orderID := uuid.NewString()
		body, err := json.Marshal(order{
			OrderID:        orderID,
			CustomerID:     customers[i%len(customers)],
			SKU:            skus[(i/len(customers))%len(skus)],
			Quantity:       i%5 + 1,
			UnitPriceCents: 999 + (i%50)*10,
			Currency:       "GBP",
			PlacedAt:       time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			log.Fatalf("marshalling order: %v", err)
		}
		// Record key is the orderId string; franz-go's default partitioner hashes
		// it to a partition. The consumer's invariant is key == body.orderId.
		records = append(records, &kgo.Record{Key: []byte(orderID), Value: body})
	}

	// ProduceSync blocks until every record is acknowledged by the broker; any
	// error fails the run non-zero.
	if err := client.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		log.Fatalf("produce failed: %v", err)
	}

	log.Printf("DELIVERED %d/%d", count, count)
}
