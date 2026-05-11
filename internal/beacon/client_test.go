// Run: go test ./internal/beacon/ -run TestBeaconClient -v
package beacon_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Dubjay/lantern/config"
	"github.com/Dubjay/lantern/internal/beacon"
)

func TestBeaconClient_GetGenesis(t *testing.T) {
	// Tests that GetGenesis correctly parses the beacon API response
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/beacon/genesis" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"genesis_time":            "1695902400",
				"genesis_validators_root": "0xabc123",
				"genesis_fork_version":    "0x01017000",
			},
		})
	}))
	defer srv.Close()

	cfg := config.NewConfig()
	cfg.BeaconURL = srv.URL

	client := beacon.NewBeaconClient(*cfg)
	genesis, err := client.GetGenesis(context.Background())

	if err != nil {
		t.Fatalf("GetGenesis() error = %v", err)
	}
	if genesis.Data.GenesisTime != 1695902400 {
		t.Errorf("GenesisTime = %d, want 1695902400", genesis.Data.GenesisTime)
	}
	if genesis.Data.GenesisValidatorsRoot != "0xabc123" {
		t.Errorf("GenesisValidatorsRoot = %s, want 0xabc123", genesis.Data.GenesisValidatorsRoot)
	}
}

func TestBeaconClient_GetBlockAttestations_MissedSlot(t *testing.T) {
	// Tests that a 404 response (missed slot) returns empty slice, not an error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":404,"message":"block not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	cfg := config.NewConfig()
	cfg.BeaconURL = srv.URL
	client := beacon.NewBeaconClient(*cfg)
	attestations, err := client.GetBlockAttestations(context.Background(), 12345)

	if err != nil {
		t.Fatalf("GetBlockAttestations on 404 should not error, got: %v", err)
	}
	if len(attestations) != 0 {
		t.Errorf("expected empty attestations for missed slot, got %d", len(attestations))
	}
}

func TestBeaconClient_RetryOnServerError(t *testing.T) {
	// Tests that the client retries on 503 before succeeding
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"genesis_time":            "1695902400",
				"genesis_validators_root": "0xabc123",
				"genesis_fork_version":    "0x01017000",
			},
		})
	}))
	defer srv.Close()
	cfg := config.NewConfig()
	cfg.Timeout = 5 * time.Second
	cfg.RetryMax = 5
	cfg.RetryBackoffMin = 1 * time.Millisecond
	cfg.BeaconURL = srv.URL
	client := beacon.NewBeaconClient(*cfg)
	_, err := client.GetGenesis(context.Background())

	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

// ── TEST HELPERS ─────────────────────────────────────────────────────────────

func newTestClient(t *testing.T, handler http.HandlerFunc) (*beacon.BeaconClient, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	cfg := config.NewConfig()
	cfg.BeaconURL = srv.URL
	client := beacon.NewBeaconClient(*cfg)
	return client, srv.Close
}
