package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// decodeRustBytes decodes only the forms rustByteStringLit emits (\", \\, \xHH).
func decodeRustBytes(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case '"':
			b.WriteByte('"')
		case '\\':
			b.WriteByte('\\')
		case 'x':
			n, _ := strconv.ParseUint(s[i+1:i+3], 16, 16)
			b.WriteByte(byte(n))
			i += 2
		}
	}
	return b.String()
}

// decodeCString decodes only the forms cStringLit emits (\", \\, 3-digit \NNN).
func decodeCString(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case '"':
			b.WriteByte('"')
		case '\\':
			b.WriteByte('\\')
		default:
			// cStringLit always emits exactly 3 octal digits.
			if i+2 < len(s) {
				if n, err := strconv.ParseUint(s[i:i+3], 8, 16); err == nil {
					b.WriteByte(byte(n))
					i += 2
					continue
				}
			}
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// TestRustByteStringLit: \xHH is mandatory because a raw non-ASCII byte in b"…"
// is a Rust compile error.
func TestRustByteStringLit(t *testing.T) {
	t.Run("exact forms", func(t *testing.T) {
		cases := map[string]string{
			`"`:          `\"`,
			`\`:          `\\`,
			`abc 123_-?`: `abc 123_-?`,  // printable ASCII verbatim (incl. '?')
			"café":       `caf\xc3\xa9`, // é = UTF-8 c3 a9
			"東京":         `\xe6\x9d\xb1\xe4\xba\xac`,
		}
		for in, want := range cases {
			if got := rustByteStringLit(in); got != want {
				t.Errorf("rustByteStringLit(%q) = %q, want %q", in, got, want)
			}
		}
	})

	t.Run("round-trip all bytes", func(t *testing.T) {
		for v := 0; v < 256; v++ {
			in := string([]byte{byte(v)})
			if got := decodeRustBytes(rustByteStringLit(in)); got != in {
				t.Errorf("byte %d: round-trip got %q", v, got)
			}
		}
	})
}

// TestRustFloatLit: a bare integer literal is i32 in Rust and won't bind to
// write_count's f64 parameter.
func TestRustFloatLit(t *testing.T) {
	cases := map[float64]string{
		1:    "1.0",
		7:    "7.0",
		100:  "100.0",
		0.5:  "0.5",
		3.14: "3.14",
		0:    "0.0",
	}
	for in, want := range cases {
		got := rustFloatLit(in)
		if got != want {
			t.Errorf("rustFloatLit(%v) = %q, want %q", in, got, want)
		}
		if !strings.ContainsAny(got, ".eE") {
			t.Errorf("rustFloatLit(%v) = %q lacks decimal/exponent", in, got)
		}
	}
}

// TestCFloatLit: full precision, and always a decimal point so a whole number
// is a double literal.
func TestCFloatLit(t *testing.T) {
	cases := map[float64]string{
		1:       "1.0",
		7:       "7.0",
		100:     "100.0",
		0.5:     "0.5",
		2.5:     "2.5",
		3.14:    "3.14",
		3.14159: "3.14159", // %.1f would have truncated this to "3.1"
		0:       "0.0",
	}
	for in, want := range cases {
		got := cFloatLit(in)
		if got != want {
			t.Errorf("cFloatLit(%v) = %q, want %q", in, got, want)
		}
		if !strings.ContainsAny(got, ".eE") {
			t.Errorf("cFloatLit(%v) = %q lacks decimal/exponent", in, got)
		}
	}
}

// TestCStringLit: octal escapes are fixed width 3 so a following digit is not
// swallowed (C's \x is greedy over all hex digits; octal stops at 3).
func TestCStringLit(t *testing.T) {
	t.Run("exact forms", func(t *testing.T) {
		cases := map[string]string{
			`"`:          `\"`,
			`\`:          `\\`,
			`abc 123_-?`: `abc 123_-?`,  // printable ASCII verbatim (incl. '?'; trigraphs are gone in C++17)
			"café":       `caf\303\251`, // é = UTF-8 c3 a9 = octal 303 251
			"東京":         `\346\235\261\344\272\254`,
		}
		for in, want := range cases {
			if got := cStringLit(in); got != want {
				t.Errorf("cStringLit(%q) = %q, want %q", in, got, want)
			}
		}
	})

	t.Run("non-greedy octal", func(t *testing.T) {
		in := string([]byte{0x03, '3'}) // ETX then '3'
		enc := cStringLit(in)           // want "\0033"
		if dec := decodeCString(enc); dec != in {
			t.Errorf("non-greedy octal round-trip: enc=%q dec=%q want %q", enc, dec, in)
		}
		if enc != `\0033` {
			t.Errorf("cStringLit({0x03,'3'}) = %q, want %q", enc, `\0033`)
		}
	})

	t.Run("round-trip all bytes", func(t *testing.T) {
		for v := 0; v < 256; v++ {
			in := string([]byte{byte(v)})
			if got := decodeCString(cStringLit(in)); got != in {
				t.Errorf("byte %d: round-trip got %q", v, got)
			}
		}
	})
}

// TestRenderRustDriverQuoting: a quote/backslash/UTF-8 value must reach the
// rendered template escaped, with no raw non-ASCII.
func TestRenderRustDriverQuoting(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const tricky = "e2e_\"q\"_\\_café_東京"
	base := uint32(1_700_000_000)
	stream := metricStream{
		Base:    base,
		Writes:  []metricWrite{{Kind: kindCounter, Metric: tricky, Tags: []tag{{"0", tricky}}, Count: 1, TS: base}},
		Metrics: []metricModel{{Name: tricky, Kind: kindCounter}},
	}
	out := t.TempDir()
	if err := renderRustDriver(filepath.Join(root, "e2e", driverRustDir, "main.rs.tmpl"), stream, out); err != nil {
		t.Fatalf("renderRustDriver: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(out, "main.rs"))
	if err != nil {
		t.Fatalf("read rendered driver: %v", err)
	}
	s := string(src)
	if strings.ContainsAny(s, "é東京") {
		t.Errorf("rendered rust source contains raw non-ASCII (would not compile in b\"…\")")
	}
	if !strings.Contains(s, `\xc3\xa9`) { // é
		t.Errorf("rendered rust source lost the \\xHH-escaped é")
	}
}

// TestRenderCppDriverQuoting: raw UTF-8 is valid in C++, but here it would
// mean the escape path was bypassed.
func TestRenderCppDriverQuoting(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const tricky = "e2e_\"q\"_\\_café_東京"
	base := uint32(1_700_000_000)
	stream := metricStream{
		Base:    base,
		Writes:  []metricWrite{{Kind: kindCounter, Metric: tricky, Tags: []tag{{"0", tricky}}, Count: 1, TS: base}},
		Metrics: []metricModel{{Name: tricky, Kind: kindCounter}},
	}
	out := t.TempDir()
	if err := renderCppDriver(filepath.Join(root, "e2e", driverCppDir, "main.cpp.tmpl"), stream, out); err != nil {
		t.Fatalf("renderCppDriver: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(out, "main.cpp"))
	if err != nil {
		t.Fatalf("read rendered driver: %v", err)
	}
	s := string(src)
	if strings.ContainsAny(s, "é東京") {
		t.Errorf("rendered cpp source contains raw non-ASCII (escaper bypassed)")
	}
	if !strings.Contains(s, `\303\251`) { // é as octal
		t.Errorf("rendered cpp source lost the octal-escaped é")
	}
}
