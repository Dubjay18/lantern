package processor

import (
	"context"

	"github.com/Dubjay/lantern/internal/beacon"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type Dispatcher struct {
	processors map[string]Processor
}

type IDispatcher interface {
	Register(name string, p Processor)
	Dispatch(ctx context.Context, event beacon.BeaconEvent)
}

type Processor interface {
	HandleHead(ctx context.Context, event beacon.HeadEvent)
	HandleReorg(ctx context.Context, event beacon.ReorgEvent)
	HandleFinalized(ctx context.Context, event beacon.FinalizedCheckpointEvent)
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{processors: make(map[string]Processor)}
}

func (d *Dispatcher) Register(name string, p Processor) {
	if d.processors == nil {
		d.processors = make(map[string]Processor)
	}
	d.processors[name] = p
}

func (d *Dispatcher) Dispatch(ctx context.Context, event beacon.BeaconEvent) {
	if len(d.processors) == 0 {
		return
	}

	var group errgroup.Group
	for name, processor := range d.processors {
		procName := name
		p := processor
		switch event.Type {
		case beacon.EventHead:
			if event.Head == nil {
				continue
			}
			group.Go(func() error {
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Error().Interface("panic", recovered).Str("processor", procName).Msg("processor panic")
					}
				}()
				p.HandleHead(ctx, *event.Head)
				return nil
			})
		case beacon.EventReorg:
			if event.Reorg == nil {
				continue
			}
			group.Go(func() error {
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Error().Interface("panic", recovered).Str("processor", procName).Msg("processor panic")
					}
				}()
				p.HandleReorg(ctx, *event.Reorg)
				return nil
			})
		case beacon.EventFinalized:
			if event.Finalized == nil {
				continue
			}
			group.Go(func() error {
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Error().Interface("panic", recovered).Str("processor", procName).Msg("processor panic")
					}
				}()
				p.HandleFinalized(ctx, *event.Finalized)
				return nil
			})
		default:
			continue
		}
	}

	_ = group.Wait()
}
