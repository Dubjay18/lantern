package beacon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type Handler func(event BeaconEvent)

type StreamState string

const (
	StateConnecting StreamState = "CONNECTING"
	StateStreaming  StreamState = "STREAMING"
	StateRetry      StreamState = "RETRY"
	StateDead       StreamState = "DEAD"
)

const (
	defaultBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
)

type BeaconEvent struct {
	Type      EventType
	Raw       json.RawMessage
	Head      *HeadEvent
	Reorg     *ReorgEvent
	Finalized *FinalizedCheckpointEvent
}

type SSEConsumer struct {
	client   *BeaconClient
	handlers map[EventType]Handler
}

func NewSSEConsumer(client *BeaconClient) *SSEConsumer {
	return &SSEConsumer{
		client:   client,
		handlers: make(map[EventType]Handler),
	}
}

func (c *SSEConsumer) On(eventType EventType, handler Handler) {
	c.handlers[eventType] = handler
}

func (c *SSEConsumer) Subscribe(ctx context.Context, topics []string) error {
	if len(topics) == 0 {
		topics = []string{string(EventHead), string(EventReorg), string(EventFinalized)}
	}

	state := StateDead
	backoff := defaultBackoff

	for {
		if ctx.Err() != nil {
			c.transitionState(state, StateDead, "context canceled")
			return ctx.Err()
		}

		state = c.transitionState(state, StateConnecting, "connecting")
		req, err := c.buildRequest(ctx, topics)
		if err != nil {
			state = c.transitionState(state, StateDead, "request build failed")
			return err
		}

		resp, err := c.client.httpClient.Do(req)
		if err != nil {
			state = c.transitionState(state, StateRetry, "connection failed")
			log.Warn().Err(err).Msg("sse connect error")
			if waitErr := waitWithContext(ctx, backoff); waitErr != nil {
				state = c.transitionState(state, StateDead, "context canceled")
				return waitErr
			}
			backoff = nextBackoff(backoff)
			continue
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = resp.Status
			}
			err = errors.New(msg)
			state = c.transitionState(state, StateRetry, "bad response")
			log.Warn().Err(err).Int("status", resp.StatusCode).Msg("sse connect error")
			if waitErr := waitWithContext(ctx, backoff); waitErr != nil {
				state = c.transitionState(state, StateDead, "context canceled")
				return waitErr
			}
			backoff = nextBackoff(backoff)
			continue
		}

		state = c.transitionState(state, StateStreaming, "streaming")
		backoff = defaultBackoff

		streamErr := c.consumeStream(ctx, resp.Body)
		_ = resp.Body.Close()
		if streamErr == nil {
			state = c.transitionState(state, StateRetry, "stream ended")
		} else if ctx.Err() != nil {
			state = c.transitionState(state, StateDead, "context canceled")
			return ctx.Err()
		} else {
			state = c.transitionState(state, StateRetry, "stream error")
			log.Warn().Err(streamErr).Msg("sse stream error")
		}

		if waitErr := waitWithContext(ctx, backoff); waitErr != nil {
			state = c.transitionState(state, StateDead, "context canceled")
			return waitErr
		}
		backoff = nextBackoff(backoff)
	}
}

func (c *SSEConsumer) buildRequest(ctx context.Context, topics []string) (*http.Request, error) {
	endpoint, err := url.Parse(c.client.baseURL)
	if err != nil {
		return nil, err
	}
	endpoint.Path = "/eth/v1/events"
	query := endpoint.Query()
	query.Set("topics", strings.Join(topics, ","))
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "Lantern/0.1")
	if c.client.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.client.token)
	}

	return req, nil
}

func (c *SSEConsumer) consumeStream(ctx context.Context, reader io.Reader) error {
	bufReader := bufio.NewReader(reader)
	var (
		eventType EventType
		dataBuf   bytes.Buffer
	)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line, err := bufReader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.dispatchEvent(eventType, dataBuf.Bytes())
			}
			return err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			c.dispatchEvent(eventType, dataBuf.Bytes())
			eventType = ""
			dataBuf.Reset()
			continue
		}

		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = EventType(strings.TrimSpace(line[len("event:"):]))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}
			dataBuf.WriteString(strings.TrimSpace(line[len("data:"):]))
			continue
		}
	}
}

func (c *SSEConsumer) dispatchEvent(eventType EventType, data []byte) {
	if len(data) == 0 {
		return
	}

	if eventType == "" {
		log.Warn().Msg("sse event missing type")
		return
	}

	event, err := parseBeaconEvent(eventType, data)
	if err != nil {
		log.Warn().Err(err).Str("event_type", string(eventType)).Msg("sse event parse error")
		return
	}

	handler, ok := c.handlers[event.Type]
	if !ok {
		return
	}

	handler(event)
}

func parseBeaconEvent(eventType EventType, data []byte) (BeaconEvent, error) {
	event := BeaconEvent{
		Type: eventType,
		Raw:  append([]byte(nil), data...),
	}

	switch eventType {
	case EventHead:
		var headEvent HeadEvent
		if err := json.Unmarshal(data, &headEvent); err != nil {
			return event, err
		}
		event.Head = &headEvent
	case EventReorg:
		var reorgEvent ReorgEvent
		if err := json.Unmarshal(data, &reorgEvent); err != nil {
			return event, err
		}
		event.Reorg = &reorgEvent
	case EventFinalized:
		var finalizedEvent FinalizedCheckpointEvent
		if err := json.Unmarshal(data, &finalizedEvent); err != nil {
			return event, err
		}
		event.Finalized = &finalizedEvent
	}

	return event, nil
}

func (c *SSEConsumer) transitionState(from StreamState, to StreamState, reason string) StreamState {
	log.Info().Str("from", string(from)).Str("to", string(to)).Str("reason", reason).Msg("sse state transition")
	return to
}

func nextBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return defaultBackoff
	}
	if current >= maxBackoff {
		return maxBackoff
	}

	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
