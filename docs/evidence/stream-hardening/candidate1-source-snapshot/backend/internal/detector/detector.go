package detector

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strconv"
	"time"

	"incidentlens/backend/internal/query"
)

const Version = "trace-v1"
const MaxSpans = 100000
const MaxGroups = 500
const MaxInputBytes = 32 << 20

var ErrLimit = errors.New("detector input limit exceeded")

type Store interface {
	Incidents(context.Context, time.Time, time.Time, Config, Filter) (Result, error)
}

type Config struct {
	MinSamples        uint64 `json:"min_samples"`
	LatencyRatioMilli uint64 `json:"latency_ratio_milli"`
	LatencyDeltaNS    string `json:"latency_delta_ns"`
	ErrorRateBPS      uint64 `json:"error_rate_bps"`
	ErrorIncreaseBPS  uint64 `json:"error_increase_bps"`
}

func DefaultConfig() Config { return Config{100, 2000, "100000000", 500, 500} }
func (c Config) Validate() error {
	d, e := strconv.ParseUint(c.LatencyDeltaNS, 10, 64)
	if e != nil || d == 0 || len(c.LatencyDeltaNS) > 20 || c.LatencyDeltaNS != strconv.FormatUint(d, 10) || c.MinSamples == 0 || c.MinSamples > 9007199254740991 || c.LatencyRatioMilli > 9007199254740991 || c.LatencyRatioMilli < 1000 || c.ErrorRateBPS < 1 || c.ErrorRateBPS > 10000 || c.ErrorIncreaseBPS < 1 || c.ErrorIncreaseBPS > 10000 {
		return query.ErrInvalid
	}
	return nil
}

type Filter struct {
	Service   *string
	Namespace *string
}
type Span struct {
	TraceID                        string
	SpanID                         string
	ParentSpanID                   string
	ServiceNamespace               string
	ServiceName                    string
	Operation                      string
	Kind                           uint8
	Status                         uint8
	Start                          time.Time
	Duration                       uint64
	DroppedAttributesCount         uint32
	DroppedEventsCount             uint32
	DroppedLinksCount              uint32
	ResourceDroppedAttributesCount uint32
	ScopeDroppedAttributesCount    uint32
	Events                         json.RawMessage
	Links                          json.RawMessage
}
type Identity struct {
	Namespace string `json:"service_namespace"`
	Service   string `json:"service_name"`
	Operation string `json:"operation"`
}
type Stats struct {
	Count      uint64  `json:"span_count"`
	ErrorCount uint64  `json:"error_count"`
	UnsetCount uint64  `json:"unset_count"`
	ErrorRate  float64 `json:"error_rate"`
	UnsetRate  float64 `json:"unset_rate"`
	P95        *string `json:"p95_duration_ns"`
}
type Evidence struct {
	TraceID             string     `json:"trace_id"`
	SpanID              string     `json:"span_id"`
	StartTime           time.Time  `json:"start_time"`
	DurationNS          string     `json:"duration_ns"`
	StatusCode          uint8      `json:"status_code"`
	From                time.Time  `json:"from"`
	To                  time.Time  `json:"to"`
	Caveats             []string   `json:"caveats"`
	DownstreamAnomalies []Identity `json:"downstream_anomalies"`
	ErrorPropagation    bool       `json:"error_propagation"`
}
type Operation struct {
	Identity
	State             string     `json:"state"`
	Baseline          Stats      `json:"baseline"`
	Current           Stats      `json:"current"`
	P95IncreaseNS     *string    `json:"p95_increase_ns"`
	ErrorRateIncrease *float64   `json:"error_rate_increase"`
	TriggeredRules    []string   `json:"triggered_rules"`
	Evidence          []Evidence `json:"evidence"`
	Caveats           []string   `json:"caveats"`
	ServiceRank       int        `json:"service_rank,omitempty"`
}
type Result struct {
	RuleVersion    string       `json:"rule_version"`
	Config         Config       `json:"config"`
	ObservedAt     time.Time    `json:"observed_at"`
	End            time.Time    `json:"end"`
	BaselineWindow query.Window `json:"baseline"`
	CurrentWindow  query.Window `json:"current"`
	Operations     []Operation  `json:"operations"`
	Candidates     []Operation  `json:"candidates"`
	Caveats        []string     `json:"caveats"`
}

func Windows(end time.Time) (query.Window, query.Window) {
	end = end.UTC()
	return query.Window{From: end.Add(-35 * time.Minute), To: end.Add(-5 * time.Minute)}, query.Window{From: end.Add(-5 * time.Minute), To: end}
}
func ValidateEnd(end, now time.Time) error {
	if end.IsZero() || end.After(now) || end.Add(-35*time.Minute).Before(now.Add(-7*24*time.Hour)) {
		return query.ErrInvalid
	}
	return nil
}
func Account(s Span) int {
	return 128 + len(s.TraceID) + len(s.SpanID) + len(s.ParentSpanID) + len(s.ServiceNamespace) + len(s.ServiceName) + len(s.Operation) + len(s.Events) + len(s.Links)
}

type key struct{ ns, svc, op string }

func identity(k key) Identity { return Identity{k.ns, k.svc, k.op} }
func spanKey(s Span) key      { return key{s.ServiceNamespace, s.ServiceName, s.Operation} }

type bucket struct{ base, cur []*Span }

func sortedKeys(groups map[key]*bucket) []key {
	ks := make([]key, 0, len(groups))
	for k := range groups {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return lessKey(ks[i], ks[j]) })
	return ks
}
func lessKey(a, b key) bool {
	if a.ns != b.ns {
		return a.ns < b.ns
	}
	if a.svc != b.svc {
		return a.svc < b.svc
	}
	return a.op < b.op
}
func stats(rows []*Span) Stats {
	v := Stats{Count: uint64(len(rows))}
	if len(rows) == 0 {
		return v
	}
	dur := make([]uint64, 0, len(rows))
	for _, s := range rows {
		dur = append(dur, s.Duration)
		if s.Status == 2 {
			v.ErrorCount++
		}
		if s.Status == 0 {
			v.UnsetCount++
		}
	}
	sort.Slice(dur, func(i, j int) bool { return dur[i] < dur[j] })
	p := strconv.FormatUint(dur[min(len(dur)-1, 95*len(dur)/100)], 10)
	v.P95 = &p
	v.ErrorRate = float64(v.ErrorCount) / float64(v.Count)
	v.UnsetRate = float64(v.UnsetCount) / float64(v.Count)
	return v
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func bigU(n uint64) *big.Int { return new(big.Int).SetUint64(n) }
func errorDelta(b, c Stats) *big.Rat {
	if b.Count == 0 || c.Count == 0 {
		return new(big.Rat)
	}
	x := new(big.Rat).SetFrac(bigU(c.ErrorCount), bigU(c.Count))
	return x.Sub(x, new(big.Rat).SetFrac(bigU(b.ErrorCount), bigU(b.Count)))
}
func rateGE(errors, count, threshold uint64) bool {
	return new(big.Int).Mul(bigU(errors), bigU(10000)).Cmp(new(big.Int).Mul(bigU(threshold), bigU(count))) >= 0
}
func deltaGE(b, c Stats, threshold uint64) bool {
	lhs := new(big.Int).Sub(new(big.Int).Mul(bigU(c.ErrorCount), bigU(b.Count)), new(big.Int).Mul(bigU(b.ErrorCount), bigU(c.Count)))
	rhs := new(big.Int).Mul(bigU(threshold), new(big.Int).Mul(bigU(c.Count), bigU(b.Count)))
	return new(big.Int).Mul(lhs, bigU(10000)).Cmp(rhs) >= 0
}
func latencyDelta(b, c Stats) *big.Int {
	if b.P95 == nil || c.P95 == nil {
		return nil
	}
	bv, _ := new(big.Int).SetString(*b.P95, 10)
	cv, _ := new(big.Int).SetString(*c.P95, 10)
	return cv.Sub(cv, bv)
}
func latencyFires(b, c Stats, cfg Config) bool {
	bv, _ := new(big.Int).SetString(*b.P95, 10)
	cv, _ := new(big.Int).SetString(*c.P95, 10)
	delta, _ := new(big.Int).SetString(cfg.LatencyDeltaNS, 10)
	ratio := new(big.Int).Mul(cv, bigU(1000))
	required := new(big.Int).Mul(bv, bigU(cfg.LatencyRatioMilli))
	return ratio.Cmp(required) >= 0 && new(big.Int).Sub(cv, bv).Cmp(delta) >= 0
}
func hasDrops(s *Span) bool {
	if s.DroppedAttributesCount > 0 || s.DroppedEventsCount > 0 || s.DroppedLinksCount > 0 || s.ResourceDroppedAttributesCount > 0 || s.ScopeDroppedAttributesCount > 0 {
		return true
	}
	var e struct {
		Events []struct {
			DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
		} `json:"events"`
		Links []struct {
			DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
		} `json:"links"`
	}
	_ = json.Unmarshal(s.Events, &e)
	for _, v := range e.Events {
		if v.DroppedAttributesCount > 0 {
			return true
		}
	}
	_ = json.Unmarshal(s.Links, &e)
	for _, v := range e.Links {
		if v.DroppedAttributesCount > 0 {
			return true
		}
	}
	return false
}
func addCaveat(xs []string, s string) []string {
	for _, v := range xs {
		if v == s {
			return xs
		}
	}
	return append(xs, s)
}

type traceInfo struct {
	children map[string][]*Span
	caveats  []string
}

func makeTraceInfo(ctx context.Context, rows []*Span) (*traceInfo, error) {
	t := &traceInfo{caveats: []string{}}
	roots := 0
	for i, s := range rows {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if s.ParentSpanID == "" {
			roots++
		}
		if hasDrops(s) {
			t.caveats = addCaveat(t.caveats, "source_drops")
		}
		if s.Status == 0 {
			t.caveats = addCaveat(t.caveats, "unset_status")
		}
	}
	if roots == 0 && len(rows) > 0 {
		t.caveats = addCaveat(t.caveats, "missing_root")
	}
	if len(rows) == 1 {
		if rows[0].ParentSpanID != "" {
			if rows[0].ParentSpanID == rows[0].SpanID {
				t.caveats = addCaveat(t.caveats, "cycle")
			} else {
				t.caveats = addCaveat(t.caveats, "missing_parent")
			}
		}
		sort.Strings(t.caveats)
		return t, nil
	}
	byID := make(map[string]*Span, len(rows))
	for _, s := range rows {
		byID[s.SpanID] = s
		if s.ParentSpanID != "" {
			if t.children == nil {
				t.children = map[string][]*Span{}
			}
			t.children[s.ParentSpanID] = append(t.children[s.ParentSpanID], s)
		}
	}
	for _, s := range rows {
		if s.ParentSpanID != "" {
			if _, ok := byID[s.ParentSpanID]; !ok {
				t.caveats = addCaveat(t.caveats, "missing_parent")
			}
		}
	}
	color := map[string]uint8{}
	for id := range byID {
		if color[id] != 0 {
			continue
		}
		path := []string{}
		cur := id
		for cur != "" && color[cur] == 0 {
			if len(path)%256 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			span, ok := byID[cur]
			if !ok {
				break
			}
			color[cur] = 1
			path = append(path, cur)
			cur = span.ParentSpanID
		}
		if cur != "" && color[cur] == 1 {
			t.caveats = addCaveat(t.caveats, "cycle")
		}
		for _, x := range path {
			color[x] = 2
		}
	}
	sort.Strings(t.caveats)
	return t, nil
}
func traceEvidence(ctx context.Context, s *Span, t *traceInfo, candidates map[key]bool, current query.Window, budget *responseBudget) (Evidence, error) {
	ev := Evidence{TraceID: s.TraceID, SpanID: s.SpanID, StartTime: s.Start.UTC(), DurationNS: strconv.FormatUint(s.Duration, 10), StatusCode: s.Status, From: current.From, To: current.To, Caveats: append([]string{}, t.caveats...), DownstreamAnomalies: []Identity{}}
	baseBytes, baseErr := json.Marshal(ev)
	if baseErr != nil {
		return ev, baseErr
	}
	if !budget.charge(2 * (len(baseBytes) + 1)) {
		return ev, ErrLimit
	}
	type item struct {
		span       *Span
		chainError bool
	}
	stack := []item{}
	for _, child := range t.children[s.SpanID] {
		stack = append(stack, item{child, s.Status == 2 && child.Status == 2})
	}
	visited := map[string]bool{s.SpanID: true}
	seen := map[key]bool{}
	steps := 0
	for len(stack) > 0 {
		steps++
		if steps%256 == 0 {
			if err := ctx.Err(); err != nil {
				return ev, err
			}
		}
		last := len(stack) - 1
		x := stack[last]
		stack = stack[:last]
		if visited[x.span.SpanID] {
			continue
		}
		visited[x.span.SpanID] = true
		if x.chainError {
			ev.ErrorPropagation = true
		}
		k := spanKey(*x.span)
		if x.span.Kind == 2 && candidates[k] && k != spanKey(*s) && !seen[k] {
			encoded, encodeErr := json.Marshal(identity(k))
			if encodeErr != nil {
				return ev, encodeErr
			}
			if !budget.charge(2 * (len(encoded) + 1)) {
				return ev, ErrLimit
			}
			seen[k] = true
			ev.DownstreamAnomalies = append(ev.DownstreamAnomalies, identity(k))
		}
		for _, child := range t.children[x.span.SpanID] {
			stack = append(stack, item{child, x.chainError && child.Status == 2})
		}
	}
	sort.Slice(ev.DownstreamAnomalies, func(i, j int) bool {
		a, b := ev.DownstreamAnomalies[i], ev.DownstreamAnomalies[j]
		return lessKey(key{a.Namespace, a.Service, a.Operation}, key{b.Namespace, b.Service, b.Operation})
	})
	return ev, nil
}
func Evaluate(rows []Span, end, observed time.Time, cfg Config, filter Filter) (Result, error) {
	return EvaluateContext(context.Background(), rows, end, observed, cfg, filter)
}
func EvaluateContext(ctx context.Context, rows []Span, end, observed time.Time, cfg Config, filter Filter) (Result, error) {
	out := Result{RuleVersion: Version, Config: cfg, ObservedAt: observed.UTC(), End: end.UTC(), Operations: []Operation{}, Candidates: []Operation{}, Caveats: []string{}}
	out.BaselineWindow, out.CurrentWindow = Windows(end)
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if e := cfg.Validate(); e != nil {
		return out, e
	}
	if e := ValidateEnd(end, observed); e != nil {
		return out, e
	}
	bytes := 0
	unique := map[[2]string]bool{}
	uniqueCount := 0
	groups := map[key]*bucket{}
	traceRows := map[string][]*Span{}
	for index := range rows {
		s := &rows[index]
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return out, err
			}
		}
		if s.Start.Before(out.BaselineWindow.From) || !s.Start.Before(end) {
			continue
		}
		id := [2]string{s.TraceID, s.SpanID}
		if unique[id] {
			continue
		}
		unique[id] = true
		if uniqueCount >= MaxSpans {
			return out, ErrLimit
		}
		uniqueCount++
		bytes += Account(*s)
		traceRows[s.TraceID] = append(traceRows[s.TraceID], s)
		if bytes > MaxInputBytes {
			return out, ErrLimit
		}
		if s.Kind != 2 {
			continue
		}
		k := spanKey(*s)
		b := groups[k]
		if b == nil {
			if len(groups) >= MaxGroups {
				return out, ErrLimit
			}
			b = &bucket{}
			groups[k] = b
		}
		if !s.Start.Before(out.CurrentWindow.From) {
			b.cur = append(b.cur, s)
		} else {
			b.base = append(b.base, s)
		}
	}
	traces := map[string]*traceInfo{}
	for id, spans := range traceRows {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		var traceErr error
		traces[id], traceErr = makeTraceInfo(ctx, spans)
		if traceErr != nil {
			return out, traceErr
		}
		for _, c := range traces[id].caveats {
			out.Caveats = addCaveat(out.Caveats, c)
		}
	}
	sort.Strings(out.Caveats)
	if uniqueCount == 0 {
		out.Caveats = append(out.Caveats, "no_server_coverage")
		return out, nil
	}
	if len(groups) == 0 {
		out.Caveats = append(out.Caveats, "no_server_coverage")
		return out, nil
	}
	for _, k := range sortedKeys(groups) {
		b := groups[k]
		op := Operation{Identity: identity(k), State: "normal", Baseline: stats(b.base), Current: stats(b.cur), TriggeredRules: []string{}, Evidence: []Evidence{}, Caveats: []string{}}
		if err := ctx.Err(); err != nil {
			return out, err
		}
		seenTrace := map[string]bool{}
		for _, span := range append(append([]*Span{}, b.base...), b.cur...) {
			if seenTrace[span.TraceID] {
				continue
			}
			seenTrace[span.TraceID] = true
			for _, c := range traces[span.TraceID].caveats {
				op.Caveats = addCaveat(op.Caveats, c)
			}
		}
		if op.Baseline.Count == 0 || op.Current.Count == 0 {
			op.Caveats = addCaveat(op.Caveats, "no_server_coverage")
		}
		sort.Strings(op.Caveats)
		if op.Baseline.Count > 0 && op.Current.Count > 0 {
			d := latencyDelta(op.Baseline, op.Current)
			v := d.String()
			op.P95IncreaseNS = &v
			r := errorDelta(op.Baseline, op.Current)
			f, _ := r.Float64()
			op.ErrorRateIncrease = &f
		}
		if op.Baseline.Count < cfg.MinSamples || op.Current.Count < cfg.MinSamples {
			op.State = "insufficient_evidence"
		} else {
			if latencyFires(op.Baseline, op.Current, cfg) {
				op.TriggeredRules = append(op.TriggeredRules, "latency")
			}
			if rateGE(op.Current.ErrorCount, op.Current.Count, cfg.ErrorRateBPS) && deltaGE(op.Baseline, op.Current, cfg.ErrorIncreaseBPS) {
				op.TriggeredRules = append(op.TriggeredRules, "errors")
			}
			if len(op.TriggeredRules) > 0 {
				op.State = "candidate"
			}
		}
		out.Operations = append(out.Operations, op)
	}
	candidates := map[key]bool{}
	for _, op := range out.Operations {
		if op.State == "candidate" {
			candidates[key{op.Namespace, op.Service, op.Operation}] = true
		}
	}
	reported := make([]Operation, 0, len(out.Operations))
	for _, op := range out.Operations {
		if filter.Service != nil && op.Service != *filter.Service || filter.Namespace != nil && op.Namespace != *filter.Namespace {
			continue
		}
		reported = append(reported, op)
	}
	out.Operations = reported
	budget := &responseBudget{remaining: query.MaxResponse - 4096}
	for _, op := range reported {
		encoded, encodeErr := json.Marshal(op)
		if encodeErr != nil {
			return out, encodeErr
		}
		copies := 1
		if op.State == "candidate" {
			copies = 2
		}
		if !budget.charge(copies*(len(encoded)+1) + 32) {
			return out, ErrLimit
		}
	}
	if len(reported) == 0 {
		out.Caveats = addCaveat(out.Caveats, "no_server_coverage")
		sort.Strings(out.Caveats)
	}
	for i := range out.Operations {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		op := &out.Operations[i]
		if op.State != "candidate" {
			continue
		}
		b := groups[key{op.Namespace, op.Service, op.Operation}]
		ordered := append([]*Span{}, b.cur...)
		sort.Slice(ordered, func(i, j int) bool {
			a, b := ordered[i], ordered[j]
			if (a.Status == 2) != (b.Status == 2) {
				return a.Status == 2
			}
			if a.Duration != b.Duration {
				return a.Duration > b.Duration
			}
			if !a.Start.Equal(b.Start) {
				return a.Start.Before(b.Start)
			}
			if a.TraceID != b.TraceID {
				return a.TraceID < b.TraceID
			}
			return a.SpanID < b.SpanID
		})
		seen := map[string]bool{}
		for _, s := range ordered {
			if seen[s.TraceID] {
				continue
			}
			seen[s.TraceID] = true
			evidence, evidenceErr := traceEvidence(ctx, s, traces[s.TraceID], candidates, query.Window{From: out.BaselineWindow.From, To: out.CurrentWindow.To}, budget)
			if evidenceErr != nil {
				return out, evidenceErr
			}
			op.Evidence = append(op.Evidence, evidence)
			if len(op.Evidence) == 5 {
				break
			}
		}
		out.Candidates = append(out.Candidates, *op)
	}
	sort.Slice(out.Candidates, func(i, j int) bool {
		a, b := out.Candidates[i], out.Candidates[j]
		ra := errorDelta(a.Baseline, a.Current)
		rb := errorDelta(b.Baseline, b.Current)
		if n := ra.Cmp(rb); n != 0 {
			return n > 0
		}
		da, db := latencyDelta(a.Baseline, a.Current), latencyDelta(b.Baseline, b.Current)
		if n := da.Cmp(db); n != 0 {
			return n > 0
		}
		return lessKey(key{a.Namespace, a.Service, a.Operation}, key{b.Namespace, b.Service, b.Operation})
	})
	rank := 0
	services := map[[2]string]int{}
	for i := range out.Candidates {
		k := [2]string{out.Candidates[i].Namespace, out.Candidates[i].Service}
		if services[k] == 0 {
			rank++
			services[k] = rank
		}
		out.Candidates[i].ServiceRank = services[k]
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

// ConfigFromEnv parses the five rule settings; an empty value uses the versioned default.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	c := DefaultConfig()
	var e error
	for _, item := range []struct {
		name   string
		target *uint64
	}{{"DETECTOR_MIN_SAMPLES", &c.MinSamples}, {"DETECTOR_LATENCY_RATIO_MILLI", &c.LatencyRatioMilli}, {"DETECTOR_ERROR_RATE_BPS", &c.ErrorRateBPS}, {"DETECTOR_ERROR_INCREASE_BPS", &c.ErrorIncreaseBPS}} {
		if s := getenv(item.name); s != "" {
			*item.target, e = strconv.ParseUint(s, 10, 64)
			if e != nil {
				return c, query.ErrInvalid
			}
		}
	}
	if s := getenv("DETECTOR_LATENCY_DELTA_NS"); s != "" {
		c.LatencyDeltaNS = s
	}
	return c, c.Validate()
}

// Reserve the envelope and charge each candidate twice because it is returned in
// both operations and candidates. Charging while evidence is built prevents
// bounded input strings from expanding into an unbounded serialized response.
type responseBudget struct{ remaining int }

func (b *responseBudget) charge(n int) bool {
	if n < 0 || n > b.remaining {
		return false
	}
	b.remaining -= n
	return true
}
