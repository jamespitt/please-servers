package worker

import (
	"syscall"
	"testing"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
)

func TestResourceUsageFields(t *testing.T) {
	action := &pb.Action{Platform: &pb.Platform{Properties: []*pb.Platform_Property{
		{Name: "OSFamily", Value: "linux"},
		{Name: "worker-type", Value: "heavy"},
	}}}
	command := &pb.Command{EnvironmentVariables: []*pb.Command_EnvironmentVariable{
		{Name: "NAME", Value: "e2e_test"},
		{Name: "PKG", Value: "vault/xpl/e2e"},
		{Name: "TEST_DIR", Value: "."},
	}}
	ar := &pb.ActionResult{
		ExitCode: 1,
		OutputFiles: []*pb.OutputFile{
			{Path: "a", Digest: &pb.Digest{Hash: "a", SizeBytes: 100}},
			{Path: "b", Digest: &pb.Digest{Hash: "b", SizeBytes: 50}},
		},
	}
	rusage := &syscall.Rusage{
		Utime:  syscall.Timeval{Sec: 3, Usec: 500000},
		Stime:  syscall.Timeval{Sec: 0, Usec: 500000},
		Maxrss: 2048,
	}

	fields := resourceUsageFields(action, command, ar, rusage, 2.0, 1000)

	assert.Equal(t, "//vault/xpl/e2e:e2e_test", fields["target"])
	assert.Equal(t, "test", fields["action_kind"])
	assert.Equal(t, "heavy", fields["worker_type"])
	assert.Equal(t, int32(1), fields["exit_code"])
	assert.Equal(t, 4.0, fields["cpu_s"])
	assert.Equal(t, 2.0, fields["cpu_cores"])
	assert.Equal(t, int64(2048*maximumResidentSetSizeUnit), fields["max_rss_b"])
	assert.Equal(t, int64(1000), fields["input_b"])
	assert.Equal(t, int64(150), fields["output_file_b"])
}

func TestResourceUsageFieldsMissingInfo(t *testing.T) {
	fields := resourceUsageFields(&pb.Action{}, &pb.Command{}, &pb.ActionResult{}, &syscall.Rusage{}, 0, 0)

	assert.Equal(t, "", fields["target"])
	assert.Equal(t, "build", fields["action_kind"])
	assert.Equal(t, "", fields["worker_type"])
	assert.Equal(t, 0.0, fields["cpu_cores"], "zero duration must not divide by zero")
}
