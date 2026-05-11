package beacon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Dubjay/lantern/config"
)

type BeaconClient struct {
	baseURL         string
	httpClient      *http.Client
	token           string
	Timeout         time.Duration
	RetryMax        int
	RetryBackoffMin time.Duration
}

func NewBeaconClient(cfg config.Config) *BeaconClient {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second // Default timeout of 10 seconds
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 5 // Default max retries
	}
	if cfg.RetryBackoffMin == 0 {
		cfg.RetryBackoffMin = 1 * time.Millisecond // Default backoff min of 1 second
	}
	fmt.Println(cfg.BeaconURL)
	return &BeaconClient{
		baseURL:         cfg.BeaconURL,
		httpClient:      &http.Client{},
		token:           cfg.BeaconToken,
		Timeout:         cfg.Timeout,
		RetryMax:        cfg.RetryMax,
		RetryBackoffMin: cfg.RetryBackoffMin,
	}
}

func (c *BeaconClient) GetGenesis(ctx context.Context) (*GenesisData, error) {
	const endpoint = "/eth/v1/beacon/genesis"
	backoff := time.Duration(c.RetryBackoffMin) * time.Second
	if backoff <= 0 {
		backoff = 1 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= c.RetryMax; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}

		resp, err := c.httpClient.Do(req)
		if err == nil {
			if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
				defer resp.Body.Close()

				// Parse the response body into GenesisData
				var genesisData GenesisData
				if err := json.NewDecoder(resp.Body).Decode(&genesisData); err != nil {
					return nil, err
				}

				return &genesisData, nil
			}

			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = http.StatusText(resp.StatusCode)
			}

			if resp.StatusCode < http.StatusInternalServerError {
				return nil, fmt.Errorf("beacon genesis request failed: status=%d body=%s", resp.StatusCode, msg)
			}

			lastErr = fmt.Errorf("beacon genesis request failed: status=%d body=%s", resp.StatusCode, msg)
		} else {
			lastErr = err
		}

		if attempt >= c.RetryMax {
			break
		}
		if waitErr := waitWithContext(ctx, backoff); waitErr != nil {
			return nil, waitErr
		}
		backoff = nextBackoff(backoff)
	}

	return nil, lastErr
}

type GenesisData struct {
	Data struct {
		GenesisTime           uint64 `json:"genesis_time,string"`
		GenesisValidatorsRoot string `json:"genesis_validators_root"`
		GenesisForkVersion    string `json:"genesis_fork_version"`
	} `json:"data"`
}

func (c *BeaconClient) GetBlockAttestations(ctx context.Context, slot uint64) ([]Attestation, error) {
	// Placeholder for actual implementation
	return nil, nil
}
