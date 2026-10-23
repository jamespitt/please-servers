package worker

import (
	"os/exec"
	"syscall"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/sirupsen/logrus"
)

// workerTypeProperty is the platform property that selects which worker pool an action runs on.
const workerTypeProperty = "worker-type"

// logResourceUsage logs one structured line per executed action describing what it consumed.
// The same numbers are attached to the action's metadata (see observeSysUsage) but nothing
// aggregates those, so this is what lets us rank targets by memory, CPU and disk, for example when
// deciding which of them belong on the heavy worker pool.
// It must be called once the outputs have been collected so that their sizes are known.
func (w *worker) logResourceUsage(action *pb.Action, command *pb.Command, ar *pb.ActionResult, cmd *exec.Cmd, execDuration float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Warning("Failed to log resource usage: %s", r)
		}
	}()
	if cmd.ProcessState == nil {
		return
	}
	rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	fields := resourceUsageFields(action, command, ar, rusage, execDuration, w.cachedBytes+w.downloadedBytes)
	fields["hash"] = w.actionDigest.Hash
	fields["worker"] = w.name
	logr.WithFields(fields).Info("Action resource usage")
}

// resourceUsageFields builds the log fields describing the resources an action used.
// inputBytes is the total size of the input root, all of which is materialised in the work dir.
func resourceUsageFields(action *pb.Action, command *pb.Command, ar *pb.ActionResult, rusage *syscall.Rusage, execDuration float64, inputBytes int64) logrus.Fields {
	cpuSecs := timevalSeconds(rusage.Utime) + timevalSeconds(rusage.Stime)
	var cpuCores float64
	if execDuration > 0 {
		cpuCores = cpuSecs / execDuration
	}
	var outputBytes int64
	for _, f := range ar.GetOutputFiles() {
		outputBytes += f.GetDigest().GetSizeBytes()
	}
	actionKind := "build"
	if getEnvVar(command, "TEST_DIR") != "" {
		actionKind = "test"
	}
	return logrus.Fields{
		"stage":         "resource_usage",
		"target":        targetLabel(command),
		"action_kind":   actionKind,
		"worker_type":   workerType(action, command),
		"exit_code":     ar.GetExitCode(),
		"duration_s":    execDuration,
		"cpu_s":         cpuSecs,
		"cpu_cores":     cpuCores,
		"max_rss_b":     rusage.Maxrss * maximumResidentSetSizeUnit,
		"input_b":       inputBytes,
		"output_file_b": outputBytes,
	}
}

// targetLabel returns the Please target an action was generated for, or "" if that can't be told.
// The worker is otherwise unaware of targets; Please sets these on every build and test action.
func targetLabel(command *pb.Command) string {
	pkg, name := getEnvVar(command, "PKG"), getEnvVar(command, "NAME")
	if name == "" {
		return ""
	}
	return "//" + pkg + ":" + name
}

// workerType returns the value of the worker-type platform property, or "" if it isn't set.
func workerType(action *pb.Action, command *pb.Command) string {
	for _, p := range action.GetPlatform().GetProperties() {
		if p.Name == workerTypeProperty {
			return p.Value
		}
	}
	for _, p := range command.GetPlatform().GetProperties() {
		if p.Name == workerTypeProperty {
			return p.Value
		}
	}
	return ""
}

func timevalSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}
