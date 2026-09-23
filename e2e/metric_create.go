package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// value_p never auto-creates (autocreate.go derives only counter/value/unique),
// so the harness POSTs each value_p metric to /api/metric before the client
// writes. --local-mode grants admin with no auth token; every name embeds the
// runID, so each POST creates a fresh metric.

// createValuePMetrics POSTs every value_p metric in the stream; any non-200
// fails the phase rather than letting the driver write into unmapped metrics.
func createValuePMetrics(ctx context.Context, rec *recorder, apiAddr string, stream metricStream) error {
	for _, m := range stream.Metrics {
		if m.Kind != kindValueP {
			continue
		}
		// Map every group-by tag position at creation (names "0".."47"); otherwise
		// the slow synchronizeWithJournal path maps them lazily and the first writes'
		// tag values resolve to empty in series_meta.
		tagObjs := make([]string, 0, len(m.QBKeys))
		for _, k := range m.QBKeys {
			tagObjs = append(tagObjs, fmt.Sprintf(`{"name":%q}`, k))
		}
		body := fmt.Sprintf(`{"metric":{"name":%q,"kind":"value_p","tags":[%s]}}`,
			m.Name, strings.Join(tagObjs, ","))
		url := "http://" + apiAddr + "/api/metric?s=" + m.Name
		resp, code, err := httpPostJSON(ctx, url, body)
		if err != nil {
			return fmt.Errorf("create value_p metric %s: POST %s: %w", m.Name, url, err)
		}
		if code != http.StatusOK {
			return fmt.Errorf("create value_p metric %s: HTTP %d: %s", m.Name, code, truncate(resp, 300))
		}
		rec.logf("created value_p metric %s (POST /api/metric → 200)", m.Name)
	}
	return nil
}

// httpPostJSON issues a JSON POST with a bounded timeout, mirroring httpGet.
func httpPostJSON(ctx context.Context, url, body string) (string, int, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, rerr
}
