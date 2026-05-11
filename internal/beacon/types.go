package beacon

import "time"

type EventType string
  const (
      EventHead      EventType = "head"
      EventReorg     EventType = "reorg"
      EventFinalized EventType = "finalized_checkpoint"
  )

  type HeadEvent struct {
      Slot                      uint64 `json:"slot,string"`
      Block                     string `json:"block"`          // root hex
      State                     string `json:"state"`          // root hex
      EpochTransition           bool   `json:"epoch_transition"`
      PreviousDutyDependentRoot string `json:"previous_duty_dependent_root"`
      CurrentDutyDependentRoot  string `json:"current_duty_dependent_root"`
  }

  type ReorgEvent struct {
      Slot         uint64 `json:"slot,string"`
      Depth        uint64 `json:"depth"`
      OldHeadBlock string `json:"old_head_block"`
      NewHeadBlock string `json:"new_head_block"`
      OldHeadState string `json:"old_head_state"`
      NewHeadState string `json:"new_head_state"`
      Epoch        uint64 `json:"epoch,string"`
  }

  type FinalizedCheckpointEvent struct {
      Block string `json:"block"`
      State string `json:"state"`
      Epoch uint64 `json:"epoch,string"`
  }

  type Attestation struct {
      AggregationBits string          `json:"aggregation_bits"`
      Data            AttestationData `json:"data"`
      Signature       string          `json:"signature"`
  }

  type AttestationData struct {
      Slot            uint64 `json:"slot,string"`
      Index           uint64 `json:"index,string"`
      BeaconBlockRoot string `json:"beacon_block_root"`
      Source          Checkpoint `json:"source"`
      Target          Checkpoint `json:"target"`
  }

    type Checkpoint struct {
        Epoch uint64 `json:"epoch,string"`
        Root  string `json:"root"`
    }

    type HeadSnapshot struct {
        Slot  uint64 `json:"slot,string"`
        Block string `json:"block"`
        State string `json:"state"`
        Timestamp time.Time `json:"timestamp"`
    }