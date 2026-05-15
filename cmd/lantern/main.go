package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Dubjay/lantern/config"
	"github.com/Dubjay/lantern/internal/beacon"
	"github.com/Dubjay/lantern/internal/processor"
)

func main() {
	cfg := config.NewConfig()
	// Load configuration from file or environment variables here
	aggregator := processor.NewMetricsAggregator(256)
	beaconClient := beacon.NewBeaconClient(*cfg)
	sseConsumer := beacon.NewSSEConsumer(beaconClient)
	dispatcher := processor.NewDispatcher()
	loggingProcessor := processor.NewLoggingProcessor()
	attestationProcessor := processor.NewAttestionProcessor(beaconClient, func(m processor.AttestationDelayMetric) {
		aggregator.Ingest(m)
	})
	dispatcher.Register("logging", loggingProcessor)
	dispatcher.Register("attestation", attestationProcessor)
	ctx := context.Background()

	sseConsumer.On(beacon.EventHead, func(event beacon.BeaconEvent) {
		dispatcher.Dispatch(ctx, event)
	})
	sseConsumer.On(beacon.EventReorg, func(event beacon.BeaconEvent) {
		dispatcher.Dispatch(ctx, event)
	})
	sseConsumer.On(beacon.EventFinalized, func(event beacon.BeaconEvent) {
		dispatcher.Dispatch(ctx, event)
	})

	go func() {
		if err := sseConsumer.Subscribe(ctx, nil); err != nil {
			fmt.Fprintf(os.Stderr, "Error subscribing to SSE: %v\n", err)
			os.Exit(1)
		}
	}()

	genesisData, err := beaconClient.GetGenesis(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching genesis data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Genesis Time: %d\n", genesisData.Data.GenesisTime)
	fmt.Printf("Genesis Validators Root: %s\n", genesisData.Data.GenesisValidatorsRoot)
	fmt.Printf("Genesis Fork Version: %s\n", genesisData.Data.GenesisForkVersion)
}
