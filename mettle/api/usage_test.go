package api

import (
	"strconv"
	"testing"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbru "github.com/thought-machine/please-servers/third_party/proto/resourceusage"
)

// usageResponse returns a response for an action that waited 2s in the queue, fetched inputs for 3s,
// executed for 10s using 15s of CPU and 2 GiB of memory, and took 1s to upload outputs.
func usageResponse(t *testing.T, start time.Time) *pb.ExecuteResponse {
	usage, err := ptypes.MarshalAny(&bbru.POSIXResourceUsage{
		UserTime:               &bbru.Duration{Seconds: 12, Nanos: 500_000_000},
		SystemTime:             &bbru.Duration{Seconds: 2, Nanos: 500_000_000},
		MaximumResidentSetSize: 2 * 1024 * 1024 * 1024,
		PageReclaims:           7,
		PageFaults:             3,
		Swaps:                  1,
	})
	require.NoError(t, err)
	cost, err := ptypes.MarshalAny(&bbru.MonetaryResourceUsage{})
	require.NoError(t, err)
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(start.Add(time.Duration(seconds) * time.Second))
	}
	return &pb.ExecuteResponse{
		Status: &status.Status{Code: 0},
		Result: &pb.ActionResult{
			ExitCode: 1,
			ExecutionMetadata: &pb.ExecutedActionMetadata{
				// The cost comes first to check that the usage is found among other metadata.
				AuxiliaryMetadata:              []*anypb.Any{cost, usage},
				QueuedTimestamp:                at(0),
				WorkerStartTimestamp:           at(2),
				InputFetchStartTimestamp:       at(2),
				InputFetchCompletedTimestamp:   at(5),
				ExecutionStartTimestamp:        at(5),
				ExecutionCompletedTimestamp:    at(15),
				OutputUploadStartTimestamp:     at(15),
				OutputUploadCompletedTimestamp: at(16),
			},
		},
	}
}

func TestNewUsageRecord(t *testing.T) {
	start := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	completed := start.Add(16 * time.Second)
	req := newRequesters("heavy", &pb.RequestMetadata{
		ToolDetails:             &pb.ToolDetails{ToolName: "please", ToolVersion: "17.0.0"},
		ActionId:                "action",
		ToolInvocationId:        "invocation",
		CorrelatedInvocationsId: "build",
		ActionMnemonic:          "TestRun",
		TargetId:                "//vault/core:core_test",
	})

	rec, ok := newUsageRecord("abc", "worker-xyz", req, usageResponse(t, start), completed)
	require.True(t, ok)

	assert.Equal(t, "abc", rec.ActionHash)
	assert.Equal(t, completed, rec.CompletedAt)
	assert.Equal(t, "worker-xyz", rec.Worker)
	assert.Equal(t, "heavy", rec.WorkerType)
	assert.Equal(t, "please", rec.ToolName)
	assert.Equal(t, "17.0.0", rec.ToolVersion)
	assert.Equal(t, "action", rec.ActionID)
	assert.Equal(t, "TestRun", rec.ActionMnemonic)
	assert.Equal(t, "//vault/core:core_test", rec.TargetID)
	assert.Equal(t, []string{"invocation"}, rec.ToolInvocationIDs)
	assert.Equal(t, []string{"build"}, rec.CorrelatedInvocationsIDs)
	assert.EqualValues(t, 1, rec.ExitCode)
	assert.EqualValues(t, 0, rec.StatusCode)
	assert.InDelta(t, 12.5, rec.UserCPUSeconds, 1e-9)
	assert.InDelta(t, 2.5, rec.SystemCPUSeconds, 1e-9)
	assert.InDelta(t, 1.5, rec.CPUCores, 1e-9)
	assert.InDelta(t, 2048, rec.MaxRSSMB, 1e-9)
	assert.EqualValues(t, 7, rec.MinorFaults)
	assert.EqualValues(t, 3, rec.MajorFaults)
	assert.EqualValues(t, 1, rec.Swaps)
	assert.InDelta(t, 2, rec.QueuedSeconds, 1e-9)
	assert.InDelta(t, 3, rec.FetchSeconds, 1e-9)
	assert.InDelta(t, 10, rec.ExecutionSeconds, 1e-9)
	assert.InDelta(t, 1, rec.UploadSeconds, 1e-9)
	require.NotNil(t, rec.ExecutionStartedAt)
	assert.Equal(t, start.Add(5*time.Second), *rec.ExecutionStartedAt)
	require.NotNil(t, rec.ExecutionCompletedAt)
	assert.Equal(t, start.Add(15*time.Second), *rec.ExecutionCompletedAt)
}

func TestNewUsageRecordWithoutRequesters(t *testing.T) {
	rec, ok := newUsageRecord("abc", "worker", nil, usageResponse(t, time.Now()), time.Now())
	require.True(t, ok)
	assert.Empty(t, rec.TargetID)
	assert.Empty(t, rec.WorkerType)
	assert.Empty(t, rec.ToolInvocationIDs)
}

func TestNewUsageRecordWithoutExecutionTimes(t *testing.T) {
	resp := usageResponse(t, time.Now())
	md := resp.Result.ExecutionMetadata
	md.ExecutionStartTimestamp, md.ExecutionCompletedTimestamp = nil, nil

	rec, ok := newUsageRecord("abc", "worker", nil, resp, time.Now())
	require.True(t, ok)
	assert.Zero(t, rec.ExecutionSeconds)
	assert.Zero(t, rec.CPUCores, "CPU cores can't be calculated without an execution time")
	assert.Nil(t, rec.ExecutionStartedAt)
	assert.Nil(t, rec.ExecutionCompletedAt)
}

func TestNewUsageRecordRequiresUsage(t *testing.T) {
	_, ok := newUsageRecord("abc", "worker", nil, nil, time.Now())
	assert.False(t, ok, "no response")
	_, ok = newUsageRecord("abc", "worker", nil, &pb.ExecuteResponse{}, time.Now())
	assert.False(t, ok, "no result")

	resp := usageResponse(t, time.Now())
	resp.Result.ExecutionMetadata.AuxiliaryMetadata = resp.Result.ExecutionMetadata.AuxiliaryMetadata[:1] // just the cost
	_, ok = newUsageRecord("abc", "worker", nil, resp, time.Now())
	assert.False(t, ok, "no POSIX usage")
}

func TestRequesters(t *testing.T) {
	r := newRequesters("standard", nil)
	assert.Nil(t, r.first)
	assert.Equal(t, "standard", r.workerType)

	r.add(&pb.RequestMetadata{ToolInvocationId: "a", CorrelatedInvocationsId: "build", TargetId: "//first"})
	r.add(&pb.RequestMetadata{ToolInvocationId: "b", CorrelatedInvocationsId: "build", TargetId: "//second"})
	r.add(&pb.RequestMetadata{ToolInvocationId: "a", CorrelatedInvocationsId: "build"})
	r.add(&pb.RequestMetadata{})
	r.add(nil)
	assert.Equal(t, "//first", r.first.TargetId, "the first client's metadata is the one that is kept")
	assert.Equal(t, []string{"a", "b"}, r.toolInvocationIDs)
	assert.Equal(t, []string{"build"}, r.correlatedInvocationsIDs)
}

func TestRequestersAreLimited(t *testing.T) {
	r := newRequesters("standard", nil)
	for i := range 2 * maxRequesters {
		r.add(&pb.RequestMetadata{ToolInvocationId: strconv.Itoa(i)})
	}
	assert.Len(t, r.toolInvocationIDs, maxRequesters)
	assert.Equal(t, "0", r.toolInvocationIDs[0])
}

func TestUsageFilter(t *testing.T) {
	rec := &usageRecord{MaxRSSMB: 500, ExecutionSeconds: 20}
	for name, tc := range map[string]struct {
		filter usageFilter
		keep   bool
	}{
		"no thresholds keeps everything": {usageFilter{}, true},
		"under the memory threshold":     {usageFilter{minMemoryMB: 501}, false},
		"at the memory threshold":        {usageFilter{minMemoryMB: 500}, true},
		"under the time threshold":       {usageFilter{minExecution: 21 * time.Second}, false},
		"at the time threshold":          {usageFilter{minExecution: 20 * time.Second}, true},
		"over memory but not time":       {usageFilter{minMemoryMB: 100, minExecution: time.Minute}, true},
		"over time but not memory":       {usageFilter{minMemoryMB: 1000, minExecution: time.Second}, true},
		"under both thresholds":          {usageFilter{minMemoryMB: 1000, minExecution: time.Minute}, false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.keep, tc.filter.keep(rec))
		})
	}
}
