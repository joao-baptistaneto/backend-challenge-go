package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		cmd     string
		rest    []string
		want    int
		wantErr bool
	}{
		{"up", nil, 0, false},
		{"status", nil, 0, false},
		{"down", []string{"1"}, 1, false},
		{"down", []string{"3"}, 3, false},
		{"down", []string{"-all"}, -1, false},
		{"down", nil, 0, true},
		{"down", []string{"0"}, 0, true},
		{"down", []string{"-1"}, 0, true},
		{"down", []string{"x"}, 0, true},
		{"up", []string{"extra"}, 0, true},
		{"drop", nil, 0, true},
	}
	for _, tt := range tests {
		got, err := parseCommand(tt.cmd, tt.rest)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseCommand(%q, %v): err = %v, wantErr %v", tt.cmd, tt.rest, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parseCommand(%q, %v) = %d, want %d", tt.cmd, tt.rest, got, tt.want)
		}
	}
}

func TestRunValidatesBeforeConnecting(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	var out bytes.Buffer

	if err := run(t.Context(), nil, env(nil), &out); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no args: err = %v", err)
	}
	if err := run(t.Context(), []string{"up"}, env(nil), &out); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("missing DATABASE_URL: err = %v", err)
	}
	err := run(t.Context(), []string{"up"}, env(map[string]string{
		"DATABASE_URL": "postgres://u:secret@localhost:1/db", "MIGRATE_TIMEOUT": "abc",
	}), &out)
	if err == nil || !strings.Contains(err.Error(), "MIGRATE_TIMEOUT") {
		t.Errorf("bad timeout: err = %v", err)
	}
	if strings.Contains(out.String(), "secret") {
		t.Error("output leaks DATABASE_URL password")
	}
}
