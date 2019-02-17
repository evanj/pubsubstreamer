package main

import (
	"bufio"
	"context"
	"flag"
	"log"
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
	flag.Parse()

	ctx := context.Background()
	psClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	pub := &pubSubPublisher{psClient.Topic(*topicID)}
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
