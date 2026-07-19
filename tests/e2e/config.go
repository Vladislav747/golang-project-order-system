//go:build e2e || e2e_async

package e2e

import "os"

const defaultBaseURL = "http://127.0.0.1:8080"

func baseURL() string {
	if v := os.Getenv("E2E_BASE_URL"); v != "" {
		return v
	}
	return defaultBaseURL
}
