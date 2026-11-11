package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sentBodies collects the bodies that a usageRecorder sends.
type sentBodies struct {
	mutex  sync.Mutex
	bodies [][]byte
}

func (s *sentBodies) send(ctx context.Context, body []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.bodies = append(s.bodies, body)
	return nil
}

func (s *sentBodies) len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.bodies)
}

func outcomeCount(outcome string) float64 {
	return testutil.ToFloat64(usageRecords.WithLabelValues(outcome))
}

func TestUsageRecorderPublishesJSON(t *testing.T) {
	var sent sentBodies
	r := newUsageRecorder(usageFilter{}, sent.send, usageQueueSize)
	published := outcomeCount(usagePublished)

	r.record(&usageRecord{ActionHash: "abc", TargetID: "//foo:bar", MaxRSSMB: 1536, CPUCores: 2.5, CorrelatedInvocationsIDs: []string{"build"}})
	r.stop()

	require.Len(t, sent.bodies, 1)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(sent.bodies[0], &fields))
	// These are the names of the columns in the BigQuery table.
	assert.Equal(t, "abc", fields["action_hash"])
	assert.Equal(t, "//foo:bar", fields["target_id"])
	assert.EqualValues(t, 1536, fields["max_rss_mb"])
	assert.EqualValues(t, 2.5, fields["cpu_cores"])
	assert.Equal(t, []any{"build"}, fields["correlated_invocations_ids"])
	assert.Contains(t, fields, "execution_started_at")
	assert.Equal(t, published+1, outcomeCount(usagePublished))
}

func TestUsageRecorderAppliesFilter(t *testing.T) {
	var sent sentBodies
	r := newUsageRecorder(usageFilter{minMemoryMB: 100}, sent.send, usageQueueSize)
	filtered := outcomeCount(usageFiltered)

	r.record(&usageRecord{ActionHash: "small", MaxRSSMB: 10})
	r.record(&usageRecord{ActionHash: "large", MaxRSSMB: 200})
	r.stop()

	require.Len(t, sent.bodies, 1)
	assert.Contains(t, string(sent.bodies[0]), `"action_hash":"large"`)
	assert.Equal(t, filtered+1, outcomeCount(usageFiltered))
}

func TestUsageRecorderSurvivesPublishFailures(t *testing.T) {
	var mutex sync.Mutex
	var sent []string
	r := newUsageRecorder(usageFilter{}, func(ctx context.Context, body []byte) error {
		mutex.Lock()
		defer mutex.Unlock()
		var rec usageRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return err
		}
		if rec.ActionHash == "bad" {
			return fmt.Errorf("topic unavailable")
		}
		sent = append(sent, rec.ActionHash)
		return nil
	}, usageQueueSize)
	failed := outcomeCount(usageFailed)

	r.record(&usageRecord{ActionHash: "bad"})
	r.record(&usageRecord{ActionHash: "good"})
	r.stop()

	assert.Equal(t, []string{"good"}, sent)
	assert.Equal(t, failed+1, outcomeCount(usageFailed))
}

func TestUsageRecorderDropsRecordsRatherThanBlocking(t *testing.T) {
	release := make(chan struct{})
	var sent sentBodies
	r := newUsageRecorder(usageFilter{}, func(ctx context.Context, body []byte) error {
		<-release
		return sent.send(ctx, body)
	}, 1)
	dropped := outcomeCount(usageDropped)

	// Every publisher is blocked and the queue holds one more, so most of these have nowhere to go.
	const total = 4 * numUsagePublishers
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range total {
			r.record(&usageRecord{ActionHash: "abc"})
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "record blocked while the queue was full")
	}

	close(release)
	r.stop()
	assert.Less(t, sent.len(), total)
	assert.Equal(t, float64(total-sent.len()), outcomeCount(usageDropped)-dropped)
}
