// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDuckStoreDocMatchesCode keeps docs/duck-store.md in sync with the flags
// that configure duck-store and with their defaults.
func TestDuckStoreDocMatchesCode(t *testing.T) {
	read := func(path string) string {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		return string(b)
	}
	doc := read("../../docs/duck-store.md")
	flags := map[string]bool{}
	for _, src := range []string{"../../cmd/statshouse-agg/statshouse-agg.go", "../../internal/api/config.go"} {
		for _, m := range regexp.MustCompile(`"(duck-[a-z0-9-]+|storage-backend)"`).FindAllStringSubmatch(read(src), -1) {
			flags[m[1]] = true
		}
	}
	require.GreaterOrEqual(t, len(flags), 7, "the flag scan went blind: did flag registration move?")
	for name := range flags {
		require.Contains(t, doc, "--"+name)
	}
	for _, want := range []string{
		fmt.Sprintf("%d hours", int(DefaultRetention1s.Hours())),
		fmt.Sprintf("%d days", int(DefaultRetention1m.Hours()/24)),
		fmt.Sprintf("%d MB", DefaultMemoryLimitBytes>>20),
		"`max(2, GOMAXPROCS)`",
	} {
		require.Contains(t, doc, want)
	}
	require.Zero(t, DefaultRetention1h, "the doc says the 1h tier is kept forever")
	require.True(t, strings.Contains(read("../../README.md"), "docs/duck-store.md"))
}
