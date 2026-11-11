package api

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/pubsub"
)

// usageQueueSize is how many records can be waiting to be published before further ones are dropped.
const usageQueueSize = 1000

// numUsagePublishers is the number of records that can be published concurrently.
const numUsagePublishers = 8

const (
	usagePublished = "published"
	usageFiltered  = "filtered"
	usageNoUsage   = "no_usage"
	usageDropped   = "dropped"
	usageFailed    = "failed"
)

var usageRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "usage_records_total",
	Help:      "Number of action resource usage records by outcome",
}, []string{"outcome"})

// usageRecorder publishes usage records in the background, so that recording one never holds up
// the processing of responses.
type usageRecorder struct {
	filter usageFilter
	send   func(ctx context.Context, body []byte) error
	queue  chan *usageRecord
	wg     sync.WaitGroup
}

// newUsageRecorder returns a recorder that sends each record that passes the filter as JSON.
func newUsageRecorder(filter usageFilter, send func(ctx context.Context, body []byte) error, queueSize int) *usageRecorder {
	r := &usageRecorder{
		filter: filter,
		send:   send,
		queue:  make(chan *usageRecord, queueSize),
	}
	r.wg.Add(numUsagePublishers)
	for range numUsagePublishers {
		go r.run()
	}
	return r
}

// topicSender returns a function that publishes bodies to the given topic.
func topicSender(topic *pubsub.Topic) func(ctx context.Context, body []byte) error {
	return func(ctx context.Context, body []byte) error {
		return topic.Send(ctx, &pubsub.Message{Body: body})
	}
}

// record queues a record for publishing if it passes the filter. It never blocks;
// if the queue is full the record is dropped.
func (r *usageRecorder) record(rec *usageRecord) {
	if !r.filter.keep(rec) {
		usageRecords.WithLabelValues(usageFiltered).Inc()
		return
	}
	select {
	case r.queue <- rec:
	default:
		usageRecords.WithLabelValues(usageDropped).Inc()
	}
}

// stop publishes any records still queued and waits for that to finish.
// It must not be called concurrently with, or followed by, record.
func (r *usageRecorder) stop() {
	close(r.queue)
	r.wg.Wait()
}

func (r *usageRecorder) run() {
	defer r.wg.Done()
	for rec := range r.queue {
		r.publish(rec)
	}
}

func (r *usageRecorder) publish(rec *usageRecord) {
	body, err := json.Marshal(rec)
	if err != nil {
		log.Warningf("Failed to serialise usage record for %s: %s", rec.ActionHash, err)
		usageRecords.WithLabelValues(usageFailed).Inc()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := r.send(ctx, body); err != nil {
		log.Warningf("Failed to publish usage record for %s: %s", rec.ActionHash, err)
		usageRecords.WithLabelValues(usageFailed).Inc()
		return
	}
	usageRecords.WithLabelValues(usagePublished).Inc()
}
