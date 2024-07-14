package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const numConnections = 50
const numRequests = 1_000_000

func main() {
	log.SetOutput(os.Stdout)

	caCertPath := "../tests/certs/rootCA.pem"
	clientCertPath := "../tests/certs/ulidd.client-client.pem"
	clientKeyPath := "../tests/certs/ulidd.client-client-key.pem"

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		panic(err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		panic("failed to append CA certificate")
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		panic(fmt.Errorf("failed to load client certificate and key: %w", err))
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		ServerName:   "localhost",
	}

	var wg sync.WaitGroup
	allULIDs := make(chan []ulid.ULID, numConnections)

	start := time.Now()
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := tls.Dial("tcp", "127.0.0.1:9000", tlsConfig)
			if err != nil {
				log.Fatalf("failed to dial: %v", err)
			}
			defer conn.Close()

			var prev ulid.ULID
			localULIDs := make([]ulid.ULID, 0, numRequests)

			for j := 0; j < numRequests; j++ {
				// Send request
				msg := []byte{1}
				if _, err := conn.Write(msg); err != nil {
					log.Fatalf("failed to write to connection: %v", err)
				}

				// Read response
				buffer := [16]byte{}
				if _, err := io.ReadFull(conn, buffer[:]); err != nil {
					log.Fatalf("failed to read from connection: %v", err)
				}

				id := ulid.ULID(buffer)
				if id.Compare(prev) <= 0 {
					log.Fatalf("received non-monotonic ID: %s < %s", id, prev)
				}
				prev = id

				localULIDs = append(localULIDs, id)
			}

			allULIDs <- localULIDs
		}()
	}

	wg.Wait()
	close(allULIDs)
	fmt.Printf("All tasks completed in %v\n", time.Since(start))

	// Collect all ULIDs into a single slice for global verification
	var combinedULIDs []ulid.ULID
	for localULIDs := range allULIDs {
		combinedULIDs = append(combinedULIDs, localULIDs...)
	}

	// Verify global monotonic order and uniqueness
	ulidSet := make(map[ulid.ULID]struct{})
	for _, id := range combinedULIDs {
		if _, exists := ulidSet[id]; exists {
			log.Fatalf("duplicate ULID found: %s", id)
		}
		ulidSet[id] = struct{}{}
	}

	fmt.Println("All ULIDs are unique and strictly monotonic")
}
