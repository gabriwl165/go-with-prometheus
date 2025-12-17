package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaBrokers  = "localhost:9092"
	kafkaTopic    = "test-topic"
	consumerGroup = "prometheus-consumer"
)

var (
	messageCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_processed_total",
		Help: "Total number of messages processed",
	})
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(kafkaTopic),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	go consumeMessages(ctx, client)

	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("Server starting on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatal(err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
}

func init() {
	log.Println("Registering metrics...")
	prometheus.MustRegister(messageCounter)
}

func consumeMessages(ctx context.Context, client *kgo.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}

			fetches.EachError(func(topic string, partition int32, err error) {
				log.Printf("fetch error: topic=%s partition=%d err=%v", topic, partition, err)
			})

			fetches.EachRecord(func(record *kgo.Record) {
				messageCounter.Inc()
				log.Printf("received message: %s", string(record.Value))
			})
		}
	}
}
