# Verification

## Run

```
make run
```

## What to look for

- Head events should log roughly every 12 seconds on Holesky.
- Reorg events are less frequent, but they do occur on Holesky given enough time.
- SSE state transitions (CONNECTING/STREAMING/RETRY/DEAD) appear in logs when the stream changes.

## Notes

- Head logs are emitted by the logging processor as "head event" with slot and block data.
- Reorg logs are emitted as "reorg event" with depth and head changes.
