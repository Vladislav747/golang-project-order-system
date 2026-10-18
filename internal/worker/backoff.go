package worker

import "time"

func nextRetryAt(now time.Time, attempts int, initial, max time.Duration) time.Time {
	delay := initial
	for i := 1; i < attempts; i++ {
		if delay > max/2 {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	return now.Add(delay)
}
