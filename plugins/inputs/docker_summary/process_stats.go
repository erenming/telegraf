package dockersummary

import (
	"time"

	"github.com/shirou/gopsutil/process"
)

func gatherProcessStats(pid int32, prefix string) (fields map[string]interface{}, tags map[string]string) {
	fields, tags = make(map[string]interface{}), make(map[string]string)
	proc, err := process.NewProcess(pid)
	if err != nil { // no process
		return
	}

	// If process_name tag is not already set, set to actual name
	if _, nameInTags := tags["process_name"]; !nameInTags {
		name, err := proc.Name()
		if err == nil {
			tags["process_name"] = name
		}
	}

	// If pid is not present as a tag, include it as a field.
	if _, pidInTags := tags["pid"]; !pidInTags {
		fields["pid"] = int32(proc.Pid)
	}

	numThreads, err := proc.NumThreads()
	if err == nil {
		fields[prefix+"num_threads"] = numThreads
	}

	fds, err := proc.NumFDs()
	if err == nil {
		fields[prefix+"num_fds"] = fds
	}

	if v, err := proc.OpenFiles(); err == nil {
		fields[prefix+"num_open_files"] = len(v)
	}

	if v, err := proc.Status(); err == nil {
		fields["process_status"] = v
	}

	if v, err := proc.Connections(); err == nil {
		fields[prefix+"num_connections"] = len(v)
	}

	if v, err := proc.PageFaults(); err == nil {
		fields[prefix+"minor_faults"] = v.MinorFaults
		fields[prefix+"child_major_faults"] = v.ChildMajorFaults
	}

	children, err := proc.Children()
	if err == nil {
		fields[prefix+"num_children"] = len(children)
	}

	ctx, err := proc.NumCtxSwitches()
	if err == nil {
		fields[prefix+"voluntary_context_switches"] = ctx.Voluntary
		fields[prefix+"involuntary_context_switches"] = ctx.Involuntary
	}

	io, err := proc.IOCounters()
	if err == nil {
		fields[prefix+"read_count"] = io.ReadCount
		fields[prefix+"write_count"] = io.WriteCount
		fields[prefix+"read_bytes"] = io.ReadBytes
		fields[prefix+"write_bytes"] = io.WriteBytes
	}

	cpu_time, err := proc.Times()
	if err == nil {
		fields[prefix+"cpu_time_user"] = cpu_time.User
		fields[prefix+"cpu_time_system"] = cpu_time.System
		fields[prefix+"cpu_time_idle"] = cpu_time.Idle
		fields[prefix+"cpu_time_nice"] = cpu_time.Nice
		fields[prefix+"cpu_time_iowait"] = cpu_time.Iowait
		fields[prefix+"cpu_time_irq"] = cpu_time.Irq
		fields[prefix+"cpu_time_soft_irq"] = cpu_time.Softirq
		fields[prefix+"cpu_time_steal"] = cpu_time.Steal
		fields[prefix+"cpu_time_guest"] = cpu_time.Guest
		fields[prefix+"cpu_time_guest_nice"] = cpu_time.GuestNice
	}

	cpu_perc, err := proc.Percent(time.Duration(0))
	if err == nil {
		fields[prefix+"cpu_usage"] = cpu_perc
	}

	mem, err := proc.MemoryInfo()
	if err == nil {
		fields[prefix+"memory_rss"] = mem.RSS
		fields[prefix+"memory_vms"] = mem.VMS
		fields[prefix+"memory_swap"] = mem.Swap
		fields[prefix+"memory_data"] = mem.Data
		fields[prefix+"memory_stack"] = mem.Stack
		fields[prefix+"memory_locked"] = mem.Locked
	}

	crt_time, err := proc.CreateTime()
	if err == nil {
		created := time.Unix(0, crt_time*time.Millisecond.Nanoseconds())
		fields[prefix+"up_time_s"] = int64(time.Since(created).Seconds())
	}
	return
}
