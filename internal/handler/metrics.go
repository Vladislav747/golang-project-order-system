package handler

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	reqCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_all_requests_total",
		Help: "Total number of HTTP requests",
	})
	createOrderCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_create_order_requests_total",
		Help: "Total number of Create Order HTTP requests",
	})
    createOrderDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_create_order_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
	getOrdersCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_get_orders_requests_total",
		Help: "Total number of Get Orders HTTP requests",
	})
	getOrdersDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_get_orders_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
	getOrderCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_get_order_requests_total",
		Help: "Total number of Get Order HTTP requests",
	})
	getOrderDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_get_order_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
	deleteOrderCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_delete_order_requests_total",
		Help: "Total number of Delete Order HTTP requests",
	})
	deleteOrderDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_delete_order_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
	updateOrderCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_update_order_requests_total",
		Help: "Total number of Update Order HTTP requests",
	})
	updateOrderDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_update_order_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
	createOrderAsyncCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_create_order_async_requests_total",
		Help: "Total number of Create Order Async HTTP requests",
	})
	createOrderAsyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_create_order_async_duration_requests_total",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: []float64{0.1, 0.5, 1},
	})
)

func init() {
    prometheus.MustRegister(reqCounter, createOrderCount, createOrderDuration)
	prometheus.MustRegister(getOrdersCount, getOrdersDuration)
	prometheus.MustRegister(getOrderCount, getOrderDuration)
	prometheus.MustRegister(deleteOrderCount, deleteOrderDuration)
	prometheus.MustRegister(updateOrderCount, updateOrderDuration)
	prometheus.MustRegister(createOrderAsyncCount, createOrderAsyncDuration)
}

func InstrumentCreateOrder(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		createOrderCount.Inc()
		next(w, r)
		createOrderDuration.Observe(time.Since(start).Seconds())
	}
}

func InstrumentGetOrders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		getOrdersCount.Inc()
		next(w, r)
		getOrdersDuration.Observe(time.Since(start).Seconds())
	}
}

func InstrumentGetOrder(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		getOrderCount.Inc()
		next(w, r)
		getOrderDuration.Observe(time.Since(start).Seconds())
	}
}

func InstrumentDeleteOrder(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		deleteOrderCount.Inc()
		next(w, r)
		deleteOrderDuration.Observe(time.Since(start).Seconds())
	}
}

func InstrumentUpdateOrder(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		updateOrderCount.Inc()
		next(w, r)
		updateOrderDuration.Observe(time.Since(start).Seconds())
	}
}

func InstrumentCreateOrderAsync(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCounter.Inc()
		createOrderAsyncCount.Inc()
		next(w, r)
		createOrderAsyncDuration.Observe(time.Since(start).Seconds())
	}
}