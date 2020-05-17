package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

type publisher struct{}

func (p *publisher) start() {

}

func (p *publisher) publish(message []byte) {

}

func (p *publisher) shutdown() error {
	return nil
}

type pubSubPublisher struct {
	topic *pubsub.Topic
}

func (p *pubSubPublisher) start() {
}

func (p *pubSubPublisher) publish(ctx context.Context, message []byte) {
	// copy message to avoid issues with concurrent publishes
	messageCopy := make([]byte, len(message))
	copy(messageCopy, message)
	p.topic.Publish(ctx, &pubsub.Message{Data: messageCopy})
}

func (p *pubSubPublisher) shutdown() error {
	p.topic.Stop()
	return nil
}

func main() {
	projectID := flag.String("projectID", "bigquery-tools", "GCP project ID")
	topicID := flag.String("topicID", "deleteme-benchmarking", "pubsub topic ID")
	duration := flag.Duration("duration", 0, "time to publish a burst of messages")
	goroutines := flag.Int("goroutines", 5, "number of publishing goroutines")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "--duration is required")
		os.Exit(1)
	}

	ctx := context.Background()
	psClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	topic := psClient.Topic(*topicID)
	pub.start()

	log.Printf("reading from stdin and publishing ...")
	scanner := bufio.NewScanner(os.Stdin)
	messageCount := 0
	byteCount := 0
	start := time.Now()
	for scanner.Scan() {
		message := scanner.Bytes()
		pub.publish(ctx, message)
		messageCount += 1
		byteCount += len(message)
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
	err = pub.shutdown()
	if err != nil {
		panic(err)
	}
	duration := time.Now().Sub(start)

	log.Printf("published %d messages in %s; %f msg/s; %d bytes = %f MiB/s",
		messageCount, duration.String(), float64(messageCount)/duration.Seconds(),
		byteCount, float64(byteCount)/duration.Seconds()/1024./1024.)
}
