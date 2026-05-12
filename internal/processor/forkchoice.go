package processor

import (
	"context"
	"sync"
	"time"

	"github.com/Dubjay/lantern/internal/beacon"
)

type ForkChoiceProcessor struct {
	mu         sync.RWMutex
	History    []beacon.HeadSnapshot
	reorgs     []ReorgMetric
	maxHistory int
	lastSlot   uint64

	onMetric func(metric ForkChoiceMetric)
	onReorg  func(metric ReorgMetric)
}

func NewForkChoiceProcessor() *ForkChoiceProcessor {
	return &ForkChoiceProcessor{
		History:    []beacon.HeadSnapshot{},
		reorgs:     []ReorgMetric{},
		maxHistory: 100,
		lastSlot:   0,
	}
}

func (p *ForkChoiceProcessor) HandleHead(ctx context.Context, event beacon.HeadEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	gap := uint64(0)
	if p.lastSlot != 0 && event.Slot > p.lastSlot+1 {
		gap = event.Slot - p.lastSlot - 1
	}
	// append to history, detect if slot gap > 1. emit ForkChoiceMetric
	p.History = append(p.History, beacon.HeadSnapshot{
		Slot:      event.Slot,
		Block:     event.Block,
		State:     event.State,
		Timestamp: time.Now(),
	})

	// Check for slot gap
	if len(p.History) > p.maxHistory {
		p.History = p.History[1:]
	}
	p.lastSlot = event.Slot

	// emit metric  - non-blocking
	if p.onMetric != nil {
		p.onMetric(ForkChoiceMetric{
			Slot:            event.Slot,
			BlockRoot:       event.Block,
			StateRoot:       event.State,
			EpochTransition: event.EpochTransition,
			SlotGap:         gap,
			ReceivedAt:      time.Now(),
		})
	}

	return nil

}

func (p *ForkChoiceProcessor) HandleReorg(ctx context.Context, event beacon.ReorgEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	m := ReorgMetric{
		Slot:    event.Slot,
		Depth:   event.Depth,
		OldHead: event.OldHeadBlock,
		NewHead: event.NewHeadBlock,
		Epoch:   event.Epoch,
	}
	p.reorgs = append(p.reorgs, m)

	if p.onReorg != nil {
		p.onReorg(m)
	}

	return nil
}

func (t *ForkChoiceProcessor) HandleFinalized(_ *beacon.FinalizedCheckpointEvent) error {
	return nil // not needed here
}

func (t *ForkChoiceProcessor) HeadHistory() []beacon.HeadSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cp := make([]beacon.HeadSnapshot, len(t.History))
	copy(cp, t.History)
	return cp
}

func (t *ForkChoiceProcessor) RecentReorgs() []ReorgMetric {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cp := make([]ReorgMetric, len(t.reorgs))
	copy(cp, t.reorgs)
	return cp
}
