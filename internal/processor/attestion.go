package processor

import (
	"context"
	"math"

	"github.com/Dubjay/lantern/internal/beacon"
)

type AttestionProcessor struct {
	beaconClient 	 *beacon.BeaconClient
	onMetric 	func(AttestationDelayMetric)
}



func (a *AttestionProcessor) HandleHead(ctx context.Context, event beacon.HeadEvent) {
		attestations, err := a.beaconClient.GetBlockAttestations(ctx, event.Slot)
		if err != nil {
			// Handle error
			return
		}
		DelayDistribution := make(map[uint64]uint64) // delay → count
			minDelay := uint64(math.MaxUint64)
	maxDelay := uint64(0)
	totalDelay := uint64(0)
		// Process attestation
		for _, att := range attestations {
			delay := event.Slot - att.Data.Slot
			DelayDistribution[delay]++
			totalDelay += delay 
			if delay < minDelay {
				minDelay = delay
			}

			if delay > maxDelay {
				maxDelay = delay
			}
		}

		avgDelay := float64(totalDelay) / float64(len(attestations))
	
		// emit AttestationMetric
if minDelay == math.MaxUint64 {
		minDelay = 0
	}

attMetric := 	AttestationDelayMetric{
		Slot:              event.Slot,
		TotalAttestations: len(attestations),
		MinDelay:          minDelay,
		MaxDelay:          maxDelay,
		AvgDelay:          avgDelay,
		Distribution:      DelayDistribution,
	}

	if a.onMetric != nil {
		a.onMetric(attMetric)
	}
}

func (a *AttestionProcessor) HandleReorg(ctx context.Context, event beacon.ReorgEvent) {
	// No-op for now
}

func (a *AttestionProcessor) HandleFinalized(ctx context.Context, event beacon.FinalizedCheckpointEvent) {
	// No-op for now
}