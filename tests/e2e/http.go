//go:build e2e || e2e_async

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	readyzPath     = "/readyz"
	readyzTimeout  = 60 * time.Second
	readyzInterval = 500 * time.Millisecond
)

// waitReady ждёт readiness probe уже запущенного сервиса перед suite.
// 200 — готов принимать трафик; 503 — зависимости ещё недоступны.
func waitReady(t testing.TB, client *http.Client) {
	t.Helper()

	url := baseURL() + readyzPath
	deadline := time.Now().Add(readyzTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			time.Sleep(readyzInterval)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(readyzInterval)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return
		case http.StatusServiceUnavailable:
			lastErr = fmt.Errorf("not ready: %s", bytes.TrimSpace(body))
		default:
			lastErr = fmt.Errorf("unexpected status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
		}
		time.Sleep(readyzInterval)
	}

	t.Fatalf(
		"readiness probe failed at %s: %v (запусти стек: docker compose up / make local-run)",
		url,
		lastErr,
	)
}

func doJSON(t testing.TB, client *http.Client, method, path string, body any) (int, []byte) {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, baseURL()+path, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, data
}
