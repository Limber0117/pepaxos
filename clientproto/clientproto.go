package clientproto

import "time"

// Latency represents the latency results from each client
type Latency struct {
	CommandID int32
	Latency   int64
	Timestamp time.Time
}

type Duration struct {
	ClientID int
	Duration time.Duration
}
