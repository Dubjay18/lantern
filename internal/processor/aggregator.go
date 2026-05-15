package processor

import (
	"context"
	"sort"
	"sync"

	"github.com/Dubjay/lantern/internal/beacon"
)

const slotsPerEpoch = 32

type EpochSummary struct {
	Epoch             uint64
	TotalAttestations int
	AvgInclusionDelay float64
	MissedSlots       int
	Reorgs            int
	Finalized         bool
}

type DashboardState struct {
	CurrentSlot        uint64
	CurrentEpoch       uint64
	LastFinalizedEpoch uint64
	LatestForkChoice   *ForkChoiceMetric
	LatestAttestation  *AttestationDelayMetric
	LatestReorg        *ReorgMetric
	LatestMissedSlot   *MissedSlotEvent
	EpochSummaries     []EpochSummary
}

type epochAggregate struct {
	summary             EpochSummary
	totalInclusionDelay float64
}

// MetricsAggregator consumes processor metrics and rolls them up into epoch summaries.
type MetricsAggregator struct {
	mu sync.RWMutex

	metricsCh chan any

	summaries       map[uint64]*epochAggregate
	lastForkChoice  *ForkChoiceMetric
	lastAttestation *AttestationDelayMetric
	lastReorg       *ReorgMetric
	lastMissedSlot  *MissedSlotEvent
	lastSlot        uint64
	lastEpoch       uint64
	lastFinalized   uint64
}




func NewMetricsAggregator(buffer int) *MetricsAggregator {
	if buffer <= 0 {
		buffer = 256
	}
	return &MetricsAggregator{
		metricsCh: make(chan any, buffer),
		summaries: make(map[uint64]*epochAggregate),
	}
}

func (a *MetricsAggregator) MetricsChan() chan<- any {
	return a.metricsCh
}

func (a *MetricsAggregator) Ingest(metric any) {
	a.metricsCh <- metric
}

// func (a *MetricsAggregator) ingestAttestionDelay(metric AttestationDelayMetric) {
// 	a.Ingest(metric)
// }
func (a *MetricsAggregator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case metric, ok := <-a.metricsCh:
			if !ok {
				return
			}
			a.handleMetric(metric)
		}
	}
}

func (a *MetricsAggregator) CurrentState() DashboardState {
	a.mu.RLock()
	defer a.mu.RUnlock()

	summaries := make([]EpochSummary, 0, len(a.summaries))
	for _, aggregate := range a.summaries {
		summaries = append(summaries, aggregate.summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Epoch < summaries[j].Epoch
	})

	return DashboardState{
		CurrentSlot:        a.lastSlot,
		CurrentEpoch:       a.lastEpoch,
		LastFinalizedEpoch: a.lastFinalized,
		LatestForkChoice:   cloneForkChoice(a.lastForkChoice),
		LatestAttestation:  cloneAttestation(a.lastAttestation),
		LatestReorg:        cloneReorg(a.lastReorg),
		LatestMissedSlot:   cloneMissedSlot(a.lastMissedSlot),
		EpochSummaries:     summaries,
	}
}

func (a *MetricsAggregator) handleMetric(metric any) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch m := metric.(type) {
	case ForkChoiceMetric:
		a.lastForkChoice = &m
		a.bumpSlot(m.Slot)
	case *ForkChoiceMetric:
		if m != nil {
			a.lastForkChoice = cloneForkChoice(m)
			a.bumpSlot(m.Slot)
		}
	case AttestationDelayMetric:
		a.lastAttestation = &m
		a.bumpSlot(m.Slot)
		a.addAttestation(m)
	case *AttestationDelayMetric:
		if m != nil {
			a.lastAttestation = cloneAttestation(m)
			a.bumpSlot(m.Slot)
			a.addAttestation(*m)
		}
	case MissedSlotEvent:
		a.lastMissedSlot = &m
		a.addMissedSlot(m.Slot)
	case *MissedSlotEvent:
		if m != nil {
			a.lastMissedSlot = cloneMissedSlot(m)
			a.addMissedSlot(m.Slot)
		}
	case ReorgMetric:
		a.lastReorg = &m
		a.addReorg(m)
	case *ReorgMetric:
		if m != nil {
			a.lastReorg = cloneReorg(m)
			a.addReorg(*m)
		}
	case beacon.FinalizedCheckpointEvent:
		a.markFinalized(m.Epoch)
	case *beacon.FinalizedCheckpointEvent:
		if m != nil {
			a.markFinalized(m.Epoch)
		}
	}
}

func (a *MetricsAggregator) bumpSlot(slot uint64) {
	if slot <= a.lastSlot {
		return
	}
	a.lastSlot = slot
	a.lastEpoch = slot / slotsPerEpoch
}

func (a *MetricsAggregator) addAttestation(metric AttestationDelayMetric) {
	epoch := metric.Slot / slotsPerEpoch
	aggregate := a.ensureEpoch(epoch)

	aggregate.summary.TotalAttestations += metric.TotalAttestations
	aggregate.totalInclusionDelay += float64(metric.TotalAttestations) * metric.AvgDelay
	if aggregate.summary.TotalAttestations > 0 {
		aggregate.summary.AvgInclusionDelay = aggregate.totalInclusionDelay / float64(aggregate.summary.TotalAttestations)
	}
}

func (a *MetricsAggregator) addMissedSlot(slot uint64) {
	epoch := slot / slotsPerEpoch
	aggregate := a.ensureEpoch(epoch)
	aggregate.summary.MissedSlots++
}

func (a *MetricsAggregator) addReorg(metric ReorgMetric) {
	aggregate := a.ensureEpoch(metric.Epoch)
	aggregate.summary.Reorgs++
}

func (a *MetricsAggregator) markFinalized(epoch uint64) {
	a.lastFinalized = epoch
	aggregate := a.ensureEpoch(epoch)
	aggregate.summary.Finalized = true
}

func (a *MetricsAggregator) ensureEpoch(epoch uint64) *epochAggregate {
	aggregate, ok := a.summaries[epoch]
	if ok {
		return aggregate
	}
	aggregate = &epochAggregate{
		summary: EpochSummary{Epoch: epoch},
	}
	a.summaries[epoch] = aggregate
	return aggregate
}

func cloneForkChoice(metric *ForkChoiceMetric) *ForkChoiceMetric {
	if metric == nil {
		return nil
	}
	copyMetric := *metric
	return &copyMetric
}

func cloneAttestation(metric *AttestationDelayMetric) *AttestationDelayMetric {
	if metric == nil {
		return nil
	}
	copyMetric := *metric
	if metric.Distribution != nil {
		copyMetric.Distribution = make(map[uint64]uint64, len(metric.Distribution))
		for k, v := range metric.Distribution {
			copyMetric.Distribution[k] = v
		}
	}
	return &copyMetric
}

func cloneReorg(metric *ReorgMetric) *ReorgMetric {
	if metric == nil {
		return nil
	}
	copyMetric := *metric
	return &copyMetric
}

func cloneMissedSlot(metric *MissedSlotEvent) *MissedSlotEvent {
	if metric == nil {
		return nil
	}
	copyMetric := *metric
	return &copyMetric
}
