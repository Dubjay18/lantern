package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "time"
)

func main() {
    // Genesis endpoint
    http.HandleFunc("/eth/v1/beacon/genesis", func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string]any{
            "data": map[string]any{
                "genesis_time":            "1695902400",
                "genesis_validators_root": "0xab0bdda0f85f842f431beaccf1250bf1fd7ba51b4100fd64364b6401fda85bb0",
                "genesis_fork_version":    "0x01017000",
            },
        })
    })

    // SSE events endpoint
    http.HandleFunc("/eth/v1/events", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/event-stream")
        w.Header().Set("Cache-Control", "no-cache")
        slot := uint64(1000000)
        for {
            payload, _ := json.Marshal(map[string]any{
                "slot": fmt.Sprintf("%d", slot),
                "block": fmt.Sprintf("0xdeadbeef%d", slot),
                "state": fmt.Sprintf("0xcafebabe%d", slot),
                "epoch_transition": slot%32 == 0,
                "previous_duty_dependent_root": "0x1111",
                "current_duty_dependent_root":  "0x2222",
            })
            fmt.Fprintf(w, "event: head\ndata: %s\n\n", payload)
            w.(http.Flusher).Flush()
            slot++
            time.Sleep(12 * time.Second)
        }
    })

    // Block attestations endpoint
    http.HandleFunc("/eth/v1/beacon/blocks/", func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string]any{
            "data": []map[string]any{
                {
                    "aggregation_bits": "0xff",
                    "data": map[string]any{
                        "slot":              "999999",
                        "index":             "0",
                        "beacon_block_root": "0xabc",
                        "source":            map[string]any{"epoch": "31249", "root": "0x111"},
                        "target":            map[string]any{"epoch": "31250", "root": "0x222"},
                    },
                    "signature": "0xsig",
                },
            },
        })
    })

    fmt.Println("mock beacon running on :5050")
    http.ListenAndServe(":5050", nil)
}