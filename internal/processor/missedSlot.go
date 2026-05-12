package processor

import (
	"context"
	"sync"
	"time"

	"github.com/Dubjay/lantern/internal/beacon"
	"github.com/rs/zerolog/log"
)

type MissedSlotSource string

const (
	MissedSlotSourceHeadGap MissedSlotSource = "head_gap"
	MissedSlotSourceTicker  MissedSlotSource = "ticker_gap"
)

type MissedSlotEvent struct {
	Slot       uint64
	DetectedAt time.Time
	Source     MissedSlotSource
}

type MissedSlotDetector struct {
	mu              sync.Mutex
	lastSeenSlot    uint64
	lastEmittedSlot uint64
	onMissedSlot    func(MissedSlotEvent)
}

func NewMissedSlotDetector(onMissedSlot func(MissedSlotEvent)) *MissedSlotDetector {
	return &MissedSlotDetector{onMissedSlot: onMissedSlot}
}

func (d *MissedSlotDetector) HandleHead(ctx context.Context, event beacon.HeadEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if event.Slot <= d.lastSeenSlot {
		return
	}

	if d.lastSeenSlot != 0 && event.Slot > d.lastSeenSlot+1 {
		for slot := d.lastSeenSlot + 1; slot < event.Slot; slot++ {
			d.emitMissedLocked(slot, MissedSlotSourceHeadGap)
		}
	}

	d.lastSeenSlot = event.Slot
}

func (d *MissedSlotDetector) HandleSlotTick(slot uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.lastSeenSlot == 0 {
		return
	}

	if slot <= d.lastSeenSlot+1 {
		return
	}

	start := d.lastSeenSlot + 1
	if d.lastEmittedSlot >= start {
		start = d.lastEmittedSlot + 1
	}

	for candidate := start; candidate+1 <= slot; candidate++ {
		d.emitMissedLocked(candidate, MissedSlotSourceTicker)
	}
}

func (d *MissedSlotDetector) Run(ctx context.Context, ticker <-chan uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		case slot, ok := <-ticker:
			if !ok {
				return
			}
			d.HandleSlotTick(slot)
		}
	}
}

func (d *MissedSlotDetector) HandleReorg(ctx context.Context, event beacon.ReorgEvent) {
	// No-op.
}

func (d *MissedSlotDetector) HandleFinalized(ctx context.Context, event beacon.FinalizedCheckpointEvent) {
	// No-op.
}

func (d *MissedSlotDetector) emitMissedLocked(slot uint64, source MissedSlotSource) {
	if slot == 0 || slot <= d.lastEmittedSlot {
		return
	}

	d.lastEmittedSlot = slot
	if d.onMissedSlot != nil {
		d.onMissedSlot(MissedSlotEvent{
			Slot:       slot,
			DetectedAt: time.Now(),
			Source:     source,
		})
		return
	}

	log.Warn().Uint64("slot", slot).Str("source", string(source)).Msg("missed slot detected")
}
