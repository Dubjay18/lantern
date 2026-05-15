package processor

import (
	"context"
	"math"

	"github.com/Dubjay/lantern/internal/beacon"
	"github.com/rs/zerolog/log"
)

type AttestionProcessor struct {
	beaconClient *beacon.BeaconClient
	onMetric     func(AttestationDelayMetric)
}

func (a *AttestionProcessor) HandleHead(ctx context.Context, event beacon.HeadEvent) error {
	attestations, err := a.beaconClient.GetBlockAttestations(ctx, event.Slot)
	if err != nil {
		// Handle error
		log.Warn().Err(err).Uint64("slot", event.Slot).Msg("failed to fetch block attestations")
		return nil
	}
	
if len(attestations) == 0 {
		return nil
	}
	
	attMetric := ComputeDelayMetric(event.Slot,attestations)

	if a.onMetric != nil {
		a.onMetric(attMetric)
	}

	return nil
}

func (a *AttestionProcessor) HandleReorg(ctx context.Context, event beacon.ReorgEvent) {
	// No-op for now
}

func (a *AttestionProcessor) HandleFinalized(ctx context.Context, event beacon.FinalizedCheckpointEvent) {
	// No-op for now
}

func ComputeDelayMetric(headSlot uint64, attestations []beacon.Attestation) AttestationDelayMetric {
	distribution := make(map[uint64]uint64)
	minDelay := uint64(math.MaxUint64)
	maxDelay := uint64(0)
	totalDelay := uint64(0)

	for _, a := range attestations {
		// attestation.Data.Slot = slot the attestation was created for
		// headSlot = slot the block containing this attestation was proposed
		delay := headSlot - a.Data.Slot

		distribution[delay]++
		totalDelay += delay

		if delay < minDelay {
			minDelay = delay
		}
		if delay > maxDelay {
			maxDelay = delay
		}
	}

	avgDelay := float64(totalDelay) / float64(len(attestations))

	// edge case: all attestations from same slot as block (delay=0 shouldn't happen
	// in practice but guard against underflow on the min)
	if minDelay == math.MaxUint64 {
		minDelay = 0
	}

	return AttestationDelayMetric{
		Slot:              headSlot,
		TotalAttestations: len(attestations),
		MinDelay:          minDelay,
		MaxDelay:          maxDelay,
		AvgDelay:          avgDelay,
		Distribution:      distribution,
	}
}

func NewAttestionProcessor(beaconClient *beacon.BeaconClient, onMetric func(AttestationDelayMetric)) *AttestionProcessor {
	return &AttestionProcessor{
		beaconClient: beaconClient,
		onMetric:     onMetric,
	}
}