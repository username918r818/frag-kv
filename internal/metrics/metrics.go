// Package metrics defines Prometheus metrics for fragkv.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	PutDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_put_duration_seconds",
		Help:    "End-to-end PUT latency by value size bucket",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 8),
	}, []string{"size_bucket"})

	GetDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_get_duration_seconds",
		Help:    "End-to-end GET latency by value size bucket",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 8),
	}, []string{"size_bucket"})

	FragmentPutDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_fragment_put_duration_seconds",
		Help:    "Single-fragment PUT latency on a storage node",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
	}, []string{"node_id"})

	FragmentGetDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_fragment_get_duration_seconds",
		Help:    "Single-fragment GET latency on a storage node",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
	}, []string{"node_id"})

	NodeUsedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kv_node_used_bytes",
		Help: "Bytes of fragment data stored on a node",
	}, []string{"node_id"})

	RebalanceBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_rebalance_bytes_total",
		Help: "Total bytes moved during rebalance",
	}, []string{"direction"}) // "sent" | "received"

	RecoveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kv_recovery_duration_seconds",
		Help:    "Time to restore full replication factor for one fragment",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 8),
	})

	FragmentOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_fragment_ops_total",
		Help: "Fragment-level operations",
	}, []string{"op", "node_id", "status"}) // op: put|get|delete|replicate

	ReplicasUnder = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kv_under_replicated_fragments",
		Help: "Number of fragments below target replication factor",
	})

	LiveNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kv_live_nodes_total",
		Help: "Number of UP storage nodes",
	})
)

// SizeBucket returns a human-readable label for a value size.
func SizeBucket(bytes int64) string {
	switch {
	case bytes < 1<<20:
		return "<1MB"
	case bytes < 10<<20:
		return "1-10MB"
	case bytes < 100<<20:
		return "10-100MB"
	case bytes < 1<<30:
		return "100MB-1GB"
	default:
		return ">1GB"
	}
}

// Handler returns the Prometheus HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
