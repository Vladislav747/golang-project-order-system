package utils

import (
	"context"
	"net/http"
	"time"
)

func RequestContext(r *http.Request, requestTimeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	return ctx, cancel
}
