package main

import "testing"

func TestBoundedQueryFailure(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"admission_busy", "admission_busy"},
		{"detector_input_limit", "detector_input_limit"},
		{"clickhouse_307", "clickhouse_307"},
		{"clickhouse_0001", ""},
		{"clickhouse_-1", ""},
		{"clickhouse_100000", ""},
		{"clickhouse_3\nunsafe", ""},
		{"arbitrary database error", ""},
	} {
		if got := boundedQueryFailure(tc.input); got != tc.want {
			t.Errorf("boundedQueryFailure(%q)=%q want %q", tc.input, got, tc.want)
		}
	}
}
