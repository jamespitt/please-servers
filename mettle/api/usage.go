package api

import (
	"slices"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbru "github.com/thought-machine/please-servers/third_party/proto/resourceusage"
)

// maxRequesters limits how many distinct invocation IDs we remember for one execution.
const maxRequesters = 16

// requesters records who asked for an execution. An execution is identified by its action hash, so identical
// concurrent requests share one and every caller is listed.
type requesters struct {
	workerType               string
	first                    *pb.RequestMetadata
	toolInvocationIDs        []string
	correlatedInvocationsIDs []string
}

func newRequesters(workerType string, md *pb.RequestMetadata) *requesters {
	r := &requesters{workerType: workerType}
	r.add(md)
	return r
}

// add records another client of the execution. Nil metadata is ignored.
func (r *requesters) add(md *pb.RequestMetadata) {
	if md == nil {
		return
	}
	if r.first == nil {
		r.first = md
	}
	r.toolInvocationIDs = appendUnique(r.toolInvocationIDs, md.ToolInvocationId)
	r.correlatedInvocationsIDs = appendUnique(r.correlatedInvocationsIDs, md.CorrelatedInvocationsId)
}

func appendUnique(s []string, v string) []string {
	if v == "" || len(s) >= maxRequesters || slices.Contains(s, v) {
		return s
	}
	return append(s, v)
}

// usageRecord describes the resources used by one executed action and who asked for it.
// The JSON field names are the column names of the BigQuery table that records are written to.
type usageRecord struct {
	ActionHash  string    `json:"action_hash"`
	CompletedAt time.Time `json:"completed_at"`
	Worker      string    `json:"worker"`
	WorkerType  string    `json:"worker_type"`

	// Taken from the RequestMetadata of the first client to request the execution.
	ToolName                 string   `json:"tool_name"`
	ToolVersion              string   `json:"tool_version"`
	ActionID                 string   `json:"action_id"`
	ActionMnemonic           string   `json:"action_mnemonic"`
	TargetID                 string   `json:"target_id"`
	ToolInvocationIDs        []string `json:"tool_invocation_ids"`
	CorrelatedInvocationsIDs []string `json:"correlated_invocations_ids"`

	ExitCode   int32 `json:"exit_code"`
	StatusCode int32 `json:"status_code"`

	UserCPUSeconds   float64 `json:"user_cpu_seconds"`
	SystemCPUSeconds float64 `json:"system_cpu_seconds"`
	// CPUCores is the CPU time divided by the execution time, i.e. the average number of cores in use.
	CPUCores float64 `json:"cpu_cores"`
	// MaxRSSMB is the peak resident set size of the largest process in the action, in the same unit as
	// the mettle_peak_memory_usage_mb metric.
	MaxRSSMB    float64 `json:"max_rss_mb"`
	MinorFaults int64   `json:"minor_faults"`
	MajorFaults int64   `json:"major_faults"`
	Swaps       int64   `json:"swaps"`

	QueuedSeconds    float64 `json:"queued_seconds"`
	FetchSeconds     float64 `json:"fetch_seconds"`
	ExecutionSeconds float64 `json:"execution_seconds"`
	UploadSeconds    float64 `json:"upload_seconds"`

	// The execution window, which allows reconstructing how many actions overlapped on a worker.
	ExecutionStartedAt   *time.Time `json:"execution_started_at"`
	ExecutionCompletedAt *time.Time `json:"execution_completed_at"`
}

// newUsageRecord builds the record for a completed execution. It returns false if the response carries no
// resource usage, which happens when the worker never ran the action.
func newUsageRecord(hash, worker string, req *requesters, resp *pb.ExecuteResponse, completed time.Time) (*usageRecord, bool) {
	md := resp.GetResult().GetExecutionMetadata()
	usage := posixUsage(md)
	if usage == nil {
		return nil, false
	}
	rec := &usageRecord{
		ActionHash:       hash,
		CompletedAt:      completed,
		Worker:           worker,
		ExitCode:         resp.GetResult().GetExitCode(),
		StatusCode:       resp.GetStatus().GetCode(),
		UserCPUSeconds:   seconds(usage.UserTime),
		SystemCPUSeconds: seconds(usage.SystemTime),
		MaxRSSMB:         float64(usage.MaximumResidentSetSize) / (1024 * 1024),
		MinorFaults:      usage.PageReclaims,
		MajorFaults:      usage.PageFaults,
		Swaps:            usage.Swaps,
		QueuedSeconds:    span(md.QueuedTimestamp, md.WorkerStartTimestamp),
		FetchSeconds:     span(md.InputFetchStartTimestamp, md.InputFetchCompletedTimestamp),
		ExecutionSeconds: span(md.ExecutionStartTimestamp, md.ExecutionCompletedTimestamp),
		UploadSeconds:    span(md.OutputUploadStartTimestamp, md.OutputUploadCompletedTimestamp),
	}
	if rec.ExecutionSeconds > 0 {
		rec.CPUCores = (rec.UserCPUSeconds + rec.SystemCPUSeconds) / rec.ExecutionSeconds
	}
	if md.ExecutionStartTimestamp != nil {
		t := md.ExecutionStartTimestamp.AsTime()
		rec.ExecutionStartedAt = &t
	}
	if md.ExecutionCompletedTimestamp != nil {
		t := md.ExecutionCompletedTimestamp.AsTime()
		rec.ExecutionCompletedAt = &t
	}
	if req != nil {
		rec.WorkerType = req.workerType
		rec.ToolInvocationIDs = req.toolInvocationIDs
		rec.CorrelatedInvocationsIDs = req.correlatedInvocationsIDs
		if md := req.first; md != nil {
			rec.ToolName = md.GetToolDetails().GetToolName()
			rec.ToolVersion = md.GetToolDetails().GetToolVersion()
			rec.ActionID = md.ActionId
			rec.ActionMnemonic = md.ActionMnemonic
			rec.TargetID = md.TargetId
		}
	}
	return rec, true
}

// posixUsage returns the POSIX resource usage the worker attached to an action's metadata, if any.
func posixUsage(md *pb.ExecutedActionMetadata) *bbru.POSIXResourceUsage {
	for _, a := range md.GetAuxiliaryMetadata() {
		usage := &bbru.POSIXResourceUsage{}
		if ptypes.UnmarshalAny(a, usage) == nil {
			return usage
		}
	}
	return nil
}

func seconds(d *bbru.Duration) float64 {
	return float64(d.GetSeconds()) + float64(d.GetNanos())/1e9
}

// span returns the time between two timestamps in seconds, or zero if either is missing.
func span(from, to *timestamppb.Timestamp) float64 {
	if from == nil || to == nil {
		return 0
	}
	return max(to.AsTime().Sub(from.AsTime()).Seconds(), 0)
}

// usageFilter selects which records are worth keeping. Most actions are small and quick, so recording only the
// larger ones keeps the volume manageable while retaining those that matter for sizing workers.
type usageFilter struct {
	minMemoryMB  float64
	minExecution time.Duration
}

// keep returns true if the record is at least as large as one of the thresholds.
// With no thresholds set every record is kept.
func (f usageFilter) keep(r *usageRecord) bool {
	if f.minMemoryMB <= 0 && f.minExecution <= 0 {
		return true
	}
	return (f.minMemoryMB > 0 && r.MaxRSSMB >= f.minMemoryMB) ||
		(f.minExecution > 0 && r.ExecutionSeconds >= f.minExecution.Seconds())
}
