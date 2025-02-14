package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/casualjim/ulidd/katotonic"
	"github.com/charmbracelet/log"
	"github.com/oklog/ulid/v2"
)

func init() {
	slog.SetDefault(slog.New(log.NewWithOptions(os.Stderr, log.Options{
		Level: log.InfoLevel,
	})))
}

const numConnections = 50
const numRequests = 1_000_000

func main() {

	caCertPath := "../tests/certs/rootCA.pem"
	clientCertPath := "../tests/certs/ulidd.client-client.pem"
	clientKeyPath := "../tests/certs/ulidd.client-client-key.pem"

	client, err := katotonic.New(
		katotonic.WithAddr("localhost:9000"),
		katotonic.WithMaxConn(numConnections),
		katotonic.WithCACert(caCertPath),
		katotonic.WithCert(clientCertPath),
		katotonic.WithKey(clientKeyPath),
	)
	if err != nil {
		slog.Error("failed to create client", slog.Any("error", err))
		os.Exit(1)
	}

	var wg sync.WaitGroup
	allULIDs := make(chan []ulid.ULID, numConnections)

	start := time.Now()
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var prev ulid.ULID
			localULIDs := make([]ulid.ULID, 0, numRequests)

			for j := 0; j < numRequests; j++ {
				if slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
					slog.Debug("Requesting next ID")
				} else if j%100_000 == 0 {
					slog.Info("Requesting next ID", slog.Int("request", j))
				}
				id, err := client.NextId()
				if err != nil {
					slog.Error("failed to get next ID", slog.Any("error", err))
					os.Exit(1)
				}
				if id.Compare(prev) <= 0 {
					slog.Error(fmt.Sprintf("received non-monotonic ID: %s < %s", id, prev))
					os.Exit(1)
				}
				prev = id

				localULIDs = append(localULIDs, id)
				slog.Debug("Received ID", slog.Any("id", id))
			}

			allULIDs <- localULIDs
		}()
	}

	wg.Wait()
	close(allULIDs)
	slog.Info("All tasks completed", slog.Duration("took", time.Since(start)))

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

	slog.Info("All ULIDs are unique and strictly monotonic")
}
