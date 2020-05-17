package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"

	"cloud.google.com/go/pubsub/apiv1"
)

const pullTimeout = 5 * time.Second

func main() {
	projectID := flag.String("projectID", "bigquery-tools", "GCP project ID")
	subscriptionID := flag.String("subscriptionID", "deleteme-benchmarking", "pubsub topic ID")
	flag.Parse()

	ctx := context.Background()
	subClient, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		panic(err)
	}

	fullSubscriptionID := fmt.Sprintf("projects/%s/subscriptions/%s", *projectID, *subscriptionID)
	// Can't set return immediately: it will return without finding anything when the messages are relatively new
	pullReq := &pubsubpb.PullRequest{
		Subscription: fullSubscriptionID,
		MaxMessages:  1000,
	}
	ackReq := &pubsubpb.AcknowledgeRequest{
		Subscription: fullSubscriptionID,
	}
	// pullReq.
	log.Printf("reading from %s and writing to stdout ...", fullSubscriptionID)
	start := time.Now()
	messageCount := 0
	byteCount := 0
	last := start
	for {
		ctxTimeout, cancel := context.WithTimeout(ctx, pullTimeout)
		resp, err := subClient.Pull(ctxTimeout, pullReq)
		cancel()
		if err != nil {
			if err == context.DeadlineExceeded {
				break
			}
			panic(err)
		}
		if len(resp.ReceivedMessages) == 0 {
			break
		}

		ackReq.AckIds = ackReq.AckIds[:0]
		for _, m := range resp.ReceivedMessages {
			os.Stdout.Write(m.Message.Data)
			os.Stdout.WriteString("\n")
			messageCount += 1
			byteCount += len(m.Message.Data)
			ackReq.AckIds = append(ackReq.AckIds, m.AckId)
		}

		err = subClient.Acknowledge(ctx, ackReq)
		if err != nil {
			panic(err)
		}
		log.Printf("acked %d messages", len(ackReq.AckIds))
		last = time.Now()
	}
	duration := last.Sub(start)

	log.Printf("pulled %d messages in %s; %f msg/s; %d bytes = %f MiB/s",
		messageCount, duration.String(), float64(messageCount)/duration.Seconds(),
		byteCount, float64(byteCount)/duration.Seconds()/1024./1024.)
}
