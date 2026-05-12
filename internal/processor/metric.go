package processor

import "time"

type ForkChoiceMetric struct {
	Slot            uint64
	BlockRoot       string
	StateRoot       string
	EpochTransition bool
	SlotGap         uint64    // slots skipped since last head (0 = normal)
	ReceivedAt      time.Time
}
type ReorgMetric struct {
	Slot     uint64
	Depth    uint64
	OldHead  string
	NewHead  string
	Epoch    uint64
}

type AttestationDelayMetric struct {
	Slot             uint64
	TotalAttestations int
	MinDelay         uint64
	MaxDelay         uint64
	AvgDelay         float64
	Distribution     map[uint64]uint64 // delay → count e.g. {1:120, 2:8, 3:2}
}