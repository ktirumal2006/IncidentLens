package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queryTask struct {
	route, phase string
	scheduled    time.Time
}

func (h *Harness) queryURL(route string, known *Known) string {
	q := url.Values{}
	q.Set("namespace", h.config.Namespace)
	path := "/api/v1/" + route
	switch route {
	case "incidents":
		q.Set("end", time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano))
	case "detail":
		path = "/api/v1/traces/" + known.TraceID
		q.Del("namespace")
		q.Set("from", known.EventTime.Add(-time.Minute).Format(time.RFC3339Nano))
		q.Set("to", known.EventTime.Add(time.Minute).Format(time.RFC3339Nano))
	default:
		q.Set("from", h.start.Add(-2*time.Second).Format(time.RFC3339Nano))
		q.Set("to", time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano))
		q.Set("limit", "100")
	}
	return strings.TrimRight(h.config.API, "/") + path + "?" + q.Encode()
}
func (h *Harness) startQueries(offerCtx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if h.config.QueryRate == 0 {
		close(done)
		return done
	}
	queue := make(chan queryTask, h.config.QueryConcurrency*2)
	wg := sync.WaitGroup{}
	client := &http.Client{Timeout: 6 * time.Second}
	for i := 0; i < h.config.QueryConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range queue {
				known := h.latest.Load()
				if known == nil {
					_ = h.rec.record(Event{Kind: "query_skipped", Phase: task.phase, Route: task.route, Scheduled: task.scheduled, Status: "no_acknowledged_trace"})
					continue
				}
				started := time.Now().UTC()
				request, e := http.NewRequestWithContext(h.ctx, "GET", h.queryURL(task.route, known), nil)
				var response *http.Response
				var size int64
				statusCode := 0
				if e == nil {
					response, e = client.Do(request)
				}
				if response != nil {
					statusCode = response.StatusCode
					var readErr error
					size, readErr = io.Copy(io.Discard, io.LimitReader(response.Body, (8<<20)+1))
					if readErr != nil {
						e = readErr
					}
					response.Body.Close()
					if size > 8<<20 {
						e = io.ErrShortBuffer
					}
				}
				finished := time.Now().UTC()
				event := Event{Kind: "query", Phase: task.phase, Route: task.route, Scheduled: task.scheduled, Started: started, Finished: finished, LatencyNS: finished.Sub(started).Nanoseconds(), SchedulingDelayNS: started.Sub(task.scheduled).Nanoseconds(), HTTPStatus: statusCode, Status: strconv.Itoa(statusCode), ResponseBytes: size}
				if e != nil {
					event.Error = e.Error()
					event.Status = "transport_error"
				}
				_ = h.rec.record(event)
			}
		}()
	}
	go func() {
		defer close(done)
		mix := strings.Split(h.config.QueryMix, ",")
		for index := 0; ; index++ {
			scheduled := h.start.Add(time.Duration(float64(index) * float64(time.Second) / h.config.QueryRate))
			if !scheduled.Before(h.start.Add(h.config.Warmup+h.config.Duration)) || !waitUntil(offerCtx, scheduled) {
				break
			}
			phase := "measured"
			if scheduled.Before(h.start.Add(h.config.Warmup)) {
				phase = "warmup"
			}
			task := queryTask{mix[index%len(mix)], phase, scheduled}
			select {
			case queue <- task:
			default:
				_ = h.rec.record(Event{Kind: "query_drop", Phase: phase, Route: task.route, Scheduled: scheduled, Started: time.Now().UTC(), Status: "queue_full"})
			}
		}
		close(queue)
		wg.Wait()
	}()
	return done
}

type Probe struct {
	Batch              *Batch
	TraceID            string
	EventTime, Started time.Time
}
type ProbeResult struct {
	probe                    Probe
	seen, lastMiss, finished time.Time
	polls                    int
	censored                 bool
	reason                   string
}

func (h *Harness) startProbes() {
	for i := 0; i < 2; i++ {
		h.probeWG.Add(1)
		go func() {
			defer h.probeWG.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for probe := range h.visibility {
				result := ProbeResult{probe: probe, censored: true, reason: "deadline"}
				deadline := probe.Started.Add(h.config.VisibilityTimeout)
				for h.ctx.Err() == nil && time.Now().Before(deadline) {
					started := time.Now().UTC()
					q := url.Values{"from": {probe.EventTime.Add(-time.Second).Format(time.RFC3339Nano)}, "to": {probe.EventTime.Add(time.Second).Format(time.RFC3339Nano)}}
					u := strings.TrimRight(h.config.API, "/") + "/api/v1/traces/" + probe.TraceID + "?" + q.Encode()
					ctx, cancel := context.WithDeadline(h.ctx, deadline)
					req, e := http.NewRequestWithContext(ctx, "GET", u, nil)
					var response *http.Response
					if e == nil {
						response, e = client.Do(req)
					}
					found := false
					code := 0
					size := int64(0)
					if response != nil {
						code = response.StatusCode
						body, readErr := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
						size = int64(len(body))
						response.Body.Close()
						if readErr != nil {
							e = readErr
						}
						if code == 200 && e == nil {
							var detail struct {
								Spans []struct {
									SpanID string `json:"span_id"`
								} `json:"spans"`
								Truncated bool `json:"truncated"`
							}
							if json.Unmarshal(body, &detail) == nil && !detail.Truncated {
								mask := uint8(0)
								for _, span := range detail.Spans {
									if n, err := strconv.ParseUint(span.SpanID, 16, 8); err == nil && n >= 1 && n <= 6 {
										mask |= 1 << uint(n-1)
									}
								}
								found = mask == 63
							}
						}
					}
					cancel()
					finished := time.Now().UTC()
					result.polls++
					event := Event{Kind: "visibility_probe", Phase: probe.Batch.Phase, Batch: probe.Batch.ID, TraceID: probe.TraceID, Started: started, Finished: finished, LatencyNS: finished.Sub(started).Nanoseconds(), HTTPStatus: code, Status: strconv.Itoa(code), ResponseBytes: size}
					if e != nil {
						event.Error = e.Error()
					}
					_ = h.rec.record(event)
					if found {
						result.seen = finished
						result.finished = finished
						result.censored = false
						result.reason = "observed_expected_six_spans"
						break
					}
					result.lastMiss = finished
					if !waitUntil(h.ctx, finished.Add(200*time.Millisecond)) {
						break
					}
				}
				if result.finished.IsZero() {
					result.finished = time.Now().UTC()
				}
				h.probeMu.Lock()
				h.probes = append(h.probes, result)
				h.probeMu.Unlock()
			}
		}()
	}
}
func (h *Harness) finishProbes() {
	for _, p := range h.probes {
		ackNS := p.probe.Batch.AckNS.Load()
		event := Event{Kind: "visibility", Phase: p.probe.Batch.Phase, Batch: p.probe.Batch.ID, TraceID: p.probe.TraceID, Scheduled: p.probe.Batch.Scheduled, Started: p.probe.Started, Finished: p.finished, LastMiss: p.lastMiss, Polls: p.polls, Censored: p.censored, Status: p.reason}
		if ackNS > 0 {
			event.Ack = time.Unix(0, ackNS).UTC()
			if !p.censored {
				event.AckToVisibleNS = p.seen.Sub(event.Ack).Nanoseconds()
			}
		}
		if !p.censored {
			event.StartToVisibleNS = p.seen.Sub(p.probe.Started).Nanoseconds()
			event.LatencyNS = p.seen.Sub(p.probe.Batch.Scheduled).Nanoseconds()
		}
		_ = h.rec.record(event)
	}
}
