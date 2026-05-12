package processor

import "time"

type SlotTicker struct {
	C           chan uint64
	genesisTime int64
}

func NewSlotTicker(genesisTime int64) *SlotTicker {
	ticker := &SlotTicker{
		C:           make(chan uint64),
		genesisTime: genesisTime,
	}

	go func() {
		for {
			currentSlot := (time.Now().Unix() - ticker.genesisTime) / 12
			ticker.C <- uint64(currentSlot)
			time.Sleep(12 * time.Second)
		}
	}()

	return ticker
}
