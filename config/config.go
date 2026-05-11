package config

import (
	"os"
	"time"
)

type Config struct {
	BeaconURL string `yaml:"beacon_url"`
	BeaconToken string `yaml:"beacon_token"`
	Timeout   time.Duration  `yaml:"timeout_seconds"`
	RetryMax    int    `yaml:"retry_max"`
	RetryBackoffMin time.Duration `yaml:"retry_backoff_min_seconds"`
}

func NewConfig() *Config {
	beaconURL := "http://localhost:5050" // Default public Holesky beacon node
	//beaconURL := "https://ethereum-hoodi-beacon-api.publicnode.com" // Default public Holesky beacon node
	if envURL := os.Getenv("BEACON_URL"); envURL != "" {
		beaconURL = envURL
	}

	beaconToken := ""
	if envToken := os.Getenv("BEACON_TOKEN"); envToken != "" {
		beaconToken = envToken
	}

	return &Config{
		BeaconURL: beaconURL,
		BeaconToken: beaconToken,
	}
}