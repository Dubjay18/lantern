package processor

import (
	"context"

	"github.com/Dubjay/lantern/internal/beacon"
	"github.com/rs/zerolog/log"
)

type LoggingProcessor struct{}

func NewLoggingProcessor() *LoggingProcessor {
	return &LoggingProcessor{}
}

func (p *LoggingProcessor) HandleHead(ctx context.Context, event beacon.HeadEvent) error {
	log.Info().Uint64("slot", event.Slot).Str("block", event.Block).Str("state", event.State).Bool("epoch_transition", event.EpochTransition).Msg("head event")
	return nil
}

func (p *LoggingProcessor) HandleReorg(ctx context.Context, event beacon.ReorgEvent) {
	log.Info().Uint64("slot", event.Slot).Uint64("depth", event.Depth).Str("old_head_block", event.OldHeadBlock).Str("new_head_block", event.NewHeadBlock).Str("old_head_state", event.OldHeadState).Str("new_head_state", event.NewHeadState).Uint64("epoch", event.Epoch).Msg("reorg event")
}

func (p *LoggingProcessor) HandleFinalized(ctx context.Context, event beacon.FinalizedCheckpointEvent) {
	log.Info().Str("block", event.Block).Str("state", event.State).Uint64("epoch", event.Epoch).Msg("finalized checkpoint event")
}
