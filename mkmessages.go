package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	numMessages := flag.Int("numMessages", 100000, "Number of messages to generate")
	length := flag.Int("length", 697, "Number of bytes per line to generate")
	flag.Parse()

	const prefixLength = 10
	if *length < prefixLength {
		panic(*length)
	}

	// base64 turns 3 bytes into 4
	const base64InputGroupSize = 3
	const base64OutputGroupSize = 4
	randomStringLength := *length - prefixLength
	base64ByteLength := ((randomStringLength)*base64InputGroupSize + base64InputGroupSize - 1) / base64OutputGroupSize
	randomBytes := make([]byte, base64ByteLength)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < *numMessages; i++ {
		_, err := rand.Read(randomBytes)
		if err != nil {
			panic(err)
		}
		randomString := base64.RawStdEncoding.EncodeToString(randomBytes)[:randomStringLength]
		line := fmt.Sprintf("%08d: %s\n", i, randomString)
		if len(line) != *length+1 {
			panic(fmt.Sprintf("bad math? %d %d %s", len(line), *length, line))
		}
		os.Stdout.WriteString(line)
	}
}
