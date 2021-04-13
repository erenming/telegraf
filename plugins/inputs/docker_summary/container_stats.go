package dockersummary

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/influxdata/telegraf"
)

func (s *Summary) gatherContainerStats(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string,
	info *types.ContainerJSON, acc telegraf.Accumulator,
) (time.Time, error) {
	if info.State != nil {
		fields["status"] = info.State.Status
		fields["oomkilled"] = info.State.OOMKilled
		fields["pid"] = info.State.Pid
		fields["exitcode"] = info.State.ExitCode

		startedAt, err := time.Parse(time.RFC3339, info.State.StartedAt)
		if err == nil && !startedAt.IsZero() {
			fields["started_at"] = startedAt.UnixNano()
		}
		finishedAt, err := time.Parse(time.RFC3339, info.State.FinishedAt)
		if err == nil && !finishedAt.IsZero() {
			fields["finished_at"] = finishedAt.UnixNano()
		}

		if info.State.Health != nil {
			fields["health_status"] = info.State.Health.Status
			fields["failing_streak"] = info.ContainerJSONBase.State.Health.FailingStreak
		}
	}

	var stats *types.StatsJSON
	var daemonOSType string
	var tm time.Time
	if info.State.Running {
		resp, err := s.dockerStats(id)
		if err != nil {
			return time.Now(), fmt.Errorf("Error getting docker stats: %s", err.Error())
		}
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		if err = dec.Decode(&stats); err != nil {
			if err == io.EOF {
				return time.Now(), nil
			}
			return time.Now(), fmt.Errorf("Error decoding: %s", err.Error())
		}
		daemonOSType = resp.OSType
		tm = stats.Read
		if tm.Before(time.Unix(0, 0)) {
			tm = time.Now()
		}
	} else {
		tm = time.Now()
	}

	s.gatherContainerProcessStats(tags, fields, info, daemonOSType, acc, tm) // must be first

	s.gatherContainerMem(id, tags, fields, envs, info.HostConfig, daemonOSType, stats)
	s.gatherContainerCPU(id, tags, fields, envs, info.HostConfig, daemonOSType, stats)
	s.gatherContainerIO(id, tags, fields, envs, info.HostConfig, daemonOSType, stats)
	s.gatherContainerNet(id, tags, fields, envs, info, daemonOSType, stats)
	return tm, nil
}

func (s *Summary) gatherContainerProcessStats(tags map[string]string, fields map[string]interface{}, info *types.ContainerJSON, daemonOSType string, acc telegraf.Accumulator, now time.Time) {
	if daemonOSType == "windows" {
		return
	}

	if info != nil && info.State != nil && info.State.Pid != 0 {
		pfields, ptags := gatherProcessStats(int32(info.State.Pid), "")
		for _, k := range []string{"pod_name", "pod_namesapce", "container_id", "container_image", "image_version"} {
			if v, ok := tags[k]; ok {
				ptags[k] = v
			}
		}
		for k, v := range fields {
			pfields[k] = v
		}
		acc.AddFields("docker_container_process_stats", pfields, ptags, now)
	}
}

func (s *Summary) gatherContainerMem(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string, info *container.HostConfig,
	daemonOSType string, stats *types.StatsJSON) {
	if info != nil {
		fields["mem_allocation"] = info.Memory
		origin, err := strconv.ParseFloat(envs["DICE_MEM_ORIGIN"], 64)
		if err == nil {
			fields["mem_origin"] = origin * 1024 * 1024
		}
		request, err := strconv.ParseFloat(envs["DICE_MEM_REQUEST"], 64)
		if err == nil {
			fields["mem_allocation"] = request * 1024 * 1024
		}
		limit, err := strconv.ParseFloat(envs["DICE_MEM_LIMIT"], 64)
		if err == nil {
			fields["mem_limit"] = limit * 1024 * 1024
		}
	}
	if stats != nil {
		if daemonOSType != "windows" {
			fields["mem_limit"] = stats.MemoryStats.Limit
			fields["mem_max_usage"] = stats.MemoryStats.MaxUsage
			mem := calculateMemUsageUnixNoCache(stats.MemoryStats)
			memLimit := float64(stats.MemoryStats.Limit)
			fields["mem_usage"] = uint64(mem)
			fields["mem_usage_percent"] = calculateMemPercentUnixNoCache(memLimit, mem)
		} else {
			fields["mem_commit_bytes"] = stats.MemoryStats.Commit
			fields["mem_commit_peak_bytes"] = stats.MemoryStats.CommitPeak
			fields["mem_private_working_set"] = stats.MemoryStats.PrivateWorkingSet
		}
	}
}

func (s *Summary) gatherContainerCPU(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string, info *container.HostConfig,
	daemonOSType string, stats *types.StatsJSON) {
	var cpuLimit, cpuAlloc float64
	var findLimit, findAlloc bool
	if str, ok := envs["DICE_CPU_LIMIT"]; ok {
		val, err := strconv.ParseFloat(str, 64)
		if err == nil {
			findLimit = true
			cpuLimit = val
		}
	}
	if !findLimit && info != nil && info.CPUPeriod != 0 {
		cpuLimit = float64(info.CPUQuota) / float64(info.CPUPeriod)
	}
	fields["cpu_limit"] = cpuLimit
	if str, ok := envs["DICE_CPU_REQUEST"]; ok {
		val, err := strconv.ParseFloat(str, 64)
		if err == nil {
			findAlloc = true
			cpuAlloc = val
		}
	}
	if !findAlloc && info != nil {
		cpuAlloc = float64(info.CPUShares) / float64(1024)
	}
	origin, err := strconv.ParseFloat(envs["DICE_MEM_ORIGIN"], 64)
	if err == nil {
		fields["cpu_origin"] = origin * 1024 * 1024
	}
	fields["cpu_allocation"] = cpuAlloc
	if stats != nil {
		if daemonOSType != "windows" {
			previousCPU := stats.PreCPUStats.CPUUsage.TotalUsage
			previousSystem := stats.PreCPUStats.SystemUsage
			cpuPercent := calculateCPUPercentUnix(previousCPU, previousSystem, stats)
			fields["cpu_usage_percent"] = cpuPercent
		} else {
			cpuPercent := calculateCPUPercentWindows(stats)
			fields["cpu_usage_percent"] = cpuPercent
		}
	}
}

func (s *Summary) gatherContainerIO(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string, info *container.HostConfig,
	daemonOSType string, stats *types.StatsJSON) {
	if stats == nil {
		return
	}
	blkioStats := stats.BlkioStats

	// Make a map of devices to their block io stats
	deviceStatMap := make(map[string]map[string]uint64)
	for _, metric := range blkioStats.IoServiceBytesRecursive {
		device := fmt.Sprintf("%d:%d", metric.Major, metric.Minor)
		_, ok := deviceStatMap[device]
		if !ok {
			deviceStatMap[device] = make(map[string]uint64)
		}
		field := fmt.Sprintf("io_service_bytes_recursive_%s", strings.ToLower(metric.Op))
		deviceStatMap[device][field] = metric.Value
	}

	for _, metric := range blkioStats.IoServicedRecursive {
		device := fmt.Sprintf("%d:%d", metric.Major, metric.Minor)
		_, ok := deviceStatMap[device]
		if !ok {
			deviceStatMap[device] = make(map[string]uint64)
		}
		field := fmt.Sprintf("io_serviced_recursive_%s", strings.ToLower(metric.Op))
		deviceStatMap[device][field] = metric.Value
	}
	var (
		readBytes, writeBytes, syncBytes uint64
		reads, writes, syncs             uint64
	)
	for _, fields := range deviceStatMap {
		for field, value := range fields {
			switch field {
			case "io_service_bytes_recursive_read":
				readBytes += value
			case "io_service_bytes_recursive_write":
				writeBytes += value
			case "io_service_bytes_recursive_sync":
				syncBytes += value
			case "io_serviced_recursive_read":
				reads += value
			case "io_serviced_recursive_write":
				writes += value
			case "io_serviced_recursive_sync":
				syncs += value
			}
		}
	}
	fields["blk_read_bytes"] = readBytes
	fields["blk_write_bytes"] = writeBytes
	fields["blk_sync_bytes"] = syncBytes
	fields["blk_reads"] = reads
	fields["blk_writes"] = writes
	fields["blk_syncs"] = syncs
}

// calculateMemUsageUnixNoCache calculate memory usage of the container.
// Page cache is intentionally excluded to avoid misinterpretation of the output.
func calculateMemUsageUnixNoCache(mem types.MemoryStats) float64 {
	return float64(mem.Usage - mem.Stats["cache"])
}

func calculateMemPercentUnixNoCache(limit float64, usedNoCache float64) float64 {
	// MemoryStats.Limit will never be 0 unless the container is not running and we haven't
	// got any data from cgroup
	if limit != 0 {
		return usedNoCache / limit * 100.0
	}
	return 0
}

func calculateCPUPercentUnix(previousCPU, previousSystem uint64, v *types.StatsJSON) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(v.CPUStats.CPUUsage.TotalUsage) - float64(previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(v.CPUStats.SystemUsage) - float64(previousSystem)
		onlineCPUs  = float64(v.CPUStats.OnlineCPUs)
	)

	if onlineCPUs == 0.0 {
		onlineCPUs = float64(len(v.CPUStats.CPUUsage.PercpuUsage))
	}
	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * onlineCPUs * 100.0
	}
	return cpuPercent
}

func calculateCPUPercentWindows(v *types.StatsJSON) float64 {
	// Max number of 100ns intervals between the previous time read and now
	possIntervals := uint64(v.Read.Sub(v.PreRead).Nanoseconds()) // Start with number of ns intervals
	possIntervals /= 100                                         // Convert to number of 100ns intervals
	possIntervals *= uint64(v.NumProcs)                          // Multiple by the number of processors

	// Intervals used
	intervalsUsed := v.CPUStats.CPUUsage.TotalUsage - v.PreCPUStats.CPUUsage.TotalUsage

	// Percentage avoiding divide-by-zero
	if possIntervals > 0 {
		return float64(intervalsUsed) / float64(possIntervals) * 100.0
	}
	return 0.00
}
