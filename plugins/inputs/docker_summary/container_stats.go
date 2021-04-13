package dockersummary

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubelet"
)

type gatherContext struct {
	id           string
	podContainer *kubelet.PodContainer
	envs         map[string]string
	info         *types.ContainerJSON
	stats        *types.StatsJSON

	fields map[string]interface{}
	tags   map[string]string
}

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
	// var daemonOSType string
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
		// daemonOSType = resp.OSType
		tm = stats.Read
		if tm.Before(time.Unix(0, 0)) {
			tm = time.Now()
		}
	} else {
		tm = time.Now()
	}

	s.gatherContainerProcessStats(tags, fields, info, acc, tm) // must be first

	gtx := s.newGatherContext(id, tags, fields, envs, info, stats)
	s.gatherContainerMem(gtx)
	s.gatherContainerCPU(gtx)
	s.gatherContainerIO(gtx)
	s.gatherContainerNet(gtx)
	return tm, nil
}

func (s *Summary) newGatherContext(
	id string,
	tags map[string]string,
	fields map[string]interface{},
	envs map[string]string,
	info *types.ContainerJSON,
	stats *types.StatsJSON,
) *gatherContext {
	gtx := &gatherContext{
		id:     id,
		tags:   tags,
		fields: fields,
		envs:   envs,
		info:   info,
		stats:  stats,
	}
	if info != nil {
		podName, _ := info.Config.Labels[labelKubernetesPodName]
		podNamespace, _ := info.Config.Labels[labelKubernetesPodNamespace]
		if pc, ok := s.getPodContainer(id, kubelet.PodID{
			Name:      podName,
			Namespace: podNamespace,
		}); ok {
			gtx.podContainer = pc
		}
		podUid, _ := info.Config.Labels[labelKubernetesPodUID]
		tags["service_instance_id"] = podUid // todo 业务需要
	}
	return gtx
}

func (s *Summary) gatherContainerProcessStats(tags map[string]string, fields map[string]interface{}, info *types.ContainerJSON, acc telegraf.Accumulator, now time.Time) {

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

func getContainerMemLimit(gtx *gatherContext) float64 {
	if gtx.podContainer != nil {
		return convertQuantityFloat(gtx.podContainer.Resources.Limits.Memory, 1)
	}

	limit, err := strconv.ParseFloat(gtx.envs["DICE_MEM_LIMIT"], 64)
	if err == nil {
		return limit * 1024 * 1024
	}
	return -1
}
func getContainerMemAllocation(gtx *gatherContext) float64 {
	if gtx.podContainer != nil {
		return convertQuantityFloat(gtx.podContainer.Resources.Requests.Memory, 1)
	}

	request, err := strconv.ParseFloat(gtx.envs["DICE_MEM_REQUEST"], 64)
	if err == nil {
		return request * 1024 * 1024
	}
	if gtx.info != nil {
		return float64(gtx.info.HostConfig.Memory)
	}
	return -1
}

func (s *Summary) gatherContainerMem(gtx *gatherContext) {
	if v := getContainerMemLimit(gtx); v != -1 {
		gtx.fields["mem_limit"] = v
	}
	if v := getContainerMemAllocation(gtx); v!= -1 {
		gtx.fields["mem_allocation"] = v
	}


	origin, err := strconv.ParseFloat(gtx.envs["DICE_MEM_ORIGIN"], 64)
	if err == nil {
		gtx.fields["mem_origin"] = origin * 1024 * 1024
	}
	kmem := s.getContainerKernelInfo(gtx.info.Config.Labels[labelKubernetesPodUID], gtx.id)

	if gtx.stats != nil {
		gtx.fields["mem_limit"] = gtx.stats.MemoryStats.Limit
		gtx.fields["mem_max_usage"] = gtx.stats.MemoryStats.MaxUsage
		mem := calculateMemUsageUnixNoCache(gtx.stats.MemoryStats)
		memLimit := float64(gtx.stats.MemoryStats.Limit)
		gtx.fields["mem_usage"] = uint64(mem)
		gtx.fields["mem_usage_percent"] = calculateMemPercentUnixNoCache(memLimit, mem)
		gtx.fields["kmem_usage_bytes"] = kmem.memoryStats.UsageInBytes
	}
}

func getContainerCPULimit(gtx *gatherContext) float64 {
	if gtx.podContainer != nil {
		return convertQuantityFloat(gtx.podContainer.Resources.Limits.CPU, 1)
	}

	info := gtx.info.HostConfig
	if str, ok := gtx.envs["DICE_CPU_LIMIT"]; ok {
		val, err := strconv.ParseFloat(str, 64)
		if err == nil {
			return val
		}
	}

	if info != nil && info.CPUPeriod != 0 {
		return float64(info.CPUQuota) / float64(info.CPUPeriod)
	}
	return -1
}

func getContainerCPUAllocation(gtx *gatherContext) float64 {
	if gtx.podContainer != nil {
		return convertQuantityFloat(gtx.podContainer.Resources.Requests.CPU, 1)
	}

	if str, ok := gtx.envs["DICE_CPU_REQUEST"]; ok {
		val, err := strconv.ParseFloat(str, 64)
		if err == nil {
			return val
		}
	}
	if gtx.info.HostConfig != nil {
		return float64(gtx.info.HostConfig.CPUShares) / float64(1024)
	}

	return -1
}

func getContainerCPUOrigin(gtx *gatherContext) float64 {
	origin, err := strconv.ParseFloat(gtx.envs["DICE_CPU_ORIGIN"], 64)
	if err == nil {
		return origin * 1024 * 1024
	}
	return -1
}

func (s *Summary) gatherContainerCPU(gtx *gatherContext) {
	if v := getContainerCPULimit(gtx); v != -1 {
		gtx.fields["cpu_limit"] = v
	}

	if v := getContainerCPUAllocation(gtx); v != -1 {
		gtx.fields["cpu_allocation"] = v
	}

	if v := getContainerCPUOrigin(gtx); v != -1 {
		gtx.fields["cpu_origin"] = v
	}

	if gtx.stats != nil {
		previousCPU := gtx.stats.PreCPUStats.CPUUsage.TotalUsage
		previousSystem := gtx.stats.PreCPUStats.SystemUsage
		cpuPercent := calculateCPUPercentUnix(previousCPU, previousSystem, gtx.stats)
		gtx.fields["cpu_usage_percent"] = cpuPercent

	}
}

func (s *Summary) gatherContainerIO(gtx *gatherContext) {
	stats := gtx.stats
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
	gtx.fields["blk_read_bytes"] = readBytes
	gtx.fields["blk_write_bytes"] = writeBytes
	gtx.fields["blk_sync_bytes"] = syncBytes
	gtx.fields["blk_reads"] = reads
	gtx.fields["blk_writes"] = writes
	gtx.fields["blk_syncs"] = syncs
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
