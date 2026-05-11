// Run: go test ./internal/beacon/ -run TestSSEConsumer -v
package beacon_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Dubjay/lantern/config"
	"github.com/Dubjay/lantern/internal/beacon"
)

func TestSSEConsumer_ParsesHeadEvent(t *testing.T) {
    // Tests that a head event SSE message is parsed into a typed HeadEvent
    headPayload := `{"slot":"100","block":"0xdeadbeef","state":"0xcafebabe","epoch_transition":false,"previous_duty_dependent_root":"0x111","current_duty_dependent_root":"0x222"}`

    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/event-stream")
        w.Header().Set("Cache-Control", "no-cache")
        fmt.Fprintf(w, "event: head\ndata: %s\n\n", headPayload)
        w.(http.Flusher).Flush()
        // Hold connection open briefly then close
        time.Sleep(100 * time.Millisecond)
    }))
    defer srv.Close()
	cfg := config.NewConfig()
	cfg.BeaconURL = srv.URL
	client := beacon.NewBeaconClient(*cfg)
    received := make(chan *beacon.HeadEvent, 1)
    consumer := beacon.NewSSEConsumer(client)
    consumer.On(beacon.EventHead, func(event beacon.BeaconEvent) {
        if event.Head == nil {
            return
        }
        received <- event.Head
    })

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    go consumer.Subscribe(ctx, []string{"head"})

    select {
    case e := <-received:
        if e.Slot != 100 {
            t.Errorf("Slot = %d, want 100", e.Slot)
        }
        if e.Block != "0xdeadbeef" {
            t.Errorf("Block = %s, want 0xdeadbeef", e.Block)
        }
    case <-ctx.Done():
        t.Fatal("timed out waiting for head event")
    }
}

func TestSSEConsumer_ReconnectsOnEOF(t *testing.T) {
    // Tests that the consumer reconnects when the SSE stream closes unexpectedly
    connectCount := 0
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        connectCount++
        w.Header().Set("Content-Type", "text/event-stream")
        w.(http.Flusher).Flush()
        // Immediately close — simulates node restart
    }))
    defer srv.Close()

	cfg := config.NewConfig()
	cfg.BeaconURL = srv.URL
	client := beacon.NewBeaconClient(*cfg)

    consumer := beacon.NewSSEConsumer(client)

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    consumer.Subscribe(ctx, []string{"head"}) // blocks until ctx done

    if connectCount < 2 {
        t.Errorf("expected at least 2 connection attempts, got %d", connectCount)
    }
}