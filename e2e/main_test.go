package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestRunIDRe(t *testing.T) {
	cases := map[string]bool{
		// valid: lowercase, digits, dashes, underscores
		"20260808-173000": true,
		"run1":            true,
		"my_run-id-2":     true,
		"abc":             true,
		// invalid: path traversal / dots / uppercase / leading dash / empty / separators
		"../../x":    false,
		".hidden":    false,
		"run.id":     false,
		"RunID":      false,
		"-leading":   false,
		"":           false,
		"with/slash": false,
	}
	for in, want := range cases {
		if got := runIDRe.MatchString(in); got != want {
			t.Errorf("runIDRe.MatchString(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestSelectDrivers compares by tag because clientDriver has a func field, which
// reflect.DeepEqual never treats as equal.
func TestSelectDrivers(t *testing.T) {
	tags := func(ds []clientDriver) []string {
		out := make([]string, len(ds))
		for i, d := range ds {
			out[i] = d.tag
		}
		return out
	}

	t.Run("empty selects all in registry order", func(t *testing.T) {
		got, err := selectDrivers(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := tags(clientDrivers); !reflect.DeepEqual(tags(got), want) {
			t.Errorf("empty selection tags = %v, want %v", tags(got), want)
		}
	})

	t.Run("single tag", func(t *testing.T) {
		got, err := selectDrivers([]string{"rust"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := []string{"rust"}; !reflect.DeepEqual(tags(got), want) {
			t.Errorf("got %v, want %v", tags(got), want)
		}
	})

	t.Run("multiple tags preserve selection order", func(t *testing.T) {
		// cpp before go on purpose: order follows the selector, not the registry.
		got, err := selectDrivers([]string{"cpp", "go"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := []string{"cpp", "go"}; !reflect.DeepEqual(tags(got), want) {
			t.Errorf("got %v, want %v", tags(got), want)
		}
	})

	t.Run("full statshouse- name accepted", func(t *testing.T) {
		got, err := selectDrivers([]string{"statshouse-cpp"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].tag != "cpp" {
			t.Errorf("got %+v, want single cpp driver", got)
		}
	})

	t.Run("duplicates collapse", func(t *testing.T) {
		got, err := selectDrivers([]string{"go", "go", "statshouse-go"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].tag != "go" {
			t.Errorf("got %+v, want single go driver", got)
		}
	})

	t.Run("unknown selector errors", func(t *testing.T) {
		for _, sel := range []string{"python", "", "statshouse-zig"} {
			if _, err := selectDrivers([]string{sel}); err == nil {
				t.Errorf("selectDrivers(%q): want error, got nil", sel)
			}
		}
	})
}

func TestClientFlag(t *testing.T) {
	var c clientFlag
	if c.String() != "" {
		t.Errorf("zero-value String() = %q, want empty", c.String())
	}
	if err := c.Set("go"); err != nil {
		t.Fatalf("Set(go): %v", err)
	}
	if err := c.Set("rust"); err != nil {
		t.Fatalf("Set(rust): %v", err)
	}
	if got, want := c.String(), "go,rust"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got, want := ([]string)(c), []string{"go", "rust"}; !reflect.DeepEqual(got, want) {
		t.Errorf("values = %v, want %v", got, want)
	}
}

func TestResolveAPIAddr(t *testing.T) {
	for flagVal, want := range map[string]string{"": "127.0.0.1:10888", "10889": "127.0.0.1:10889"} {
		if got, err := resolveAPIAddr(flagVal); err != nil || got != want {
			t.Errorf("--api-port=%q: got %q, %v; want %q", flagVal, got, err, want)
		}
	}
	for _, bad := range []string{"abc", "0", "70000"} {
		if _, err := resolveAPIAddr(bad); err == nil {
			t.Errorf("--api-port=%q: want an error", bad)
		}
	}
	if got, err := resolveAPIAddr("auto"); err != nil || !strings.HasPrefix(got, "127.0.0.1:") {
		t.Errorf("--api-port=auto: got %q, %v", got, err)
	}
}
