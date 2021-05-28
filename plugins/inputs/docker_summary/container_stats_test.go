package dockersummary

import (
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGatherContainerCPUWithKubernetes(t *testing.T) {
	gtx := mockGatherContext()
	s := mockSummary()

	s.gatherContainerCPU(gtx)

	assert.Equal(t, float64(400), gtx.fields["cpu_usage_percent"])
	assert.Equal(t, 0.01, gtx.fields["cpu_allocation"])
	assert.Equal(t, 0.5*1024*1024, gtx.fields["cpu_origin"])
	assert.Equal(t, 0.5, gtx.fields["cpu_limit"])
}

func TestGatherContainerCPU(t *testing.T) {
	gtx := mockGatherContext()
	gtx.podContainer = apiv1.Container{}
	s := mockSummary()

	s.gatherContainerCPU(gtx)

	assert.Equal(t, float64(400), gtx.fields["cpu_usage_percent"])
	assert.Equal(t, 0.083333, gtx.fields["cpu_allocation"])
	assert.Equal(t, 0.5*1024*1024, gtx.fields["cpu_origin"])
	assert.Equal(t, 0.5, gtx.fields["cpu_limit"])
}

func TestGatherContainerMemWithKubernetes(t *testing.T) {
	gtx := mockGatherContext()
	s := mockSummary()

	s.gatherContainerMem(gtx)

	assert.Equal(t, float64(10*1024*1024), gtx.fields["mem_allocation"]) // 只针对该字段有意义
	assert.Equal(t, float64(512*1024*1024), gtx.fields["mem_origin"])
	assert.Equal(t, uint64(512*1024*1024), gtx.fields["mem_limit"])
	assert.Equal(t, uint64(120*1024*1024), gtx.fields["mem_max_usage"])
	assert.Equal(t, uint64(100*1024*1024), gtx.fields["mem_usage"])
	assert.Equal(t, 19.53125, gtx.fields["mem_usage_percent"])
}

func TestGatherContainerMem(t *testing.T) {
	gtx := mockGatherContext()
	gtx.podContainer = apiv1.Container{}
	s := mockSummary()

	s.gatherContainerMem(gtx)

	assert.Equal(t, float64(32*1024*1024), gtx.fields["mem_allocation"])
	assert.Equal(t, float64(512*1024*1024), gtx.fields["mem_origin"])
	assert.Equal(t, uint64(512*1024*1024), gtx.fields["mem_limit"])
	assert.Equal(t, uint64(120*1024*1024), gtx.fields["mem_max_usage"])
	assert.Equal(t, uint64(100*1024*1024), gtx.fields["mem_usage"])
	assert.Equal(t, 19.53125, gtx.fields["mem_usage_percent"])
}

func Test_getContainerMemLimit(t *testing.T) {
	gtx := mockGatherContext()

	// pod
	limit := getContainerMemLimit(gtx)
	assert.Equal(t, float64(512*1024*1024), limit)

	// env
	gtx.podContainer = apiv1.Container{}
	limit = getContainerMemLimit(gtx)
	assert.Equal(t, float64(512*1024*1024), limit)

	// none
	delete(gtx.envs, "DICE_MEM_LIMIT")
	limit = getContainerMemLimit(gtx)
	assert.Equal(t, 0.0, limit)
}

func Test_getContainerMemAllocation(t *testing.T) {
	gtx := mockGatherContext()

	// pod
	alloc := getContainerMemAllocation(gtx)
	assert.Equal(t, float64(10*1024*1024), alloc)

	// env
	gtx.podContainer = apiv1.Container{}
	alloc = getContainerMemAllocation(gtx)
	assert.Equal(t, float64(32*1024*1024), alloc)

	// info
	gtx.info.HostConfig.Memory = 24 * 1024 * 1024
	delete(gtx.envs, "DICE_MEM_REQUEST")
	alloc = getContainerMemAllocation(gtx)
	assert.Equal(t, float64(24*1024*1024), alloc)

	// none
	gtx.info.HostConfig = nil
	alloc = getContainerMemAllocation(gtx)
	assert.Equal(t, 0.0, alloc)

	gtx.info = nil
	alloc = getContainerMemAllocation(gtx)
	assert.Equal(t, 0.0, alloc)
}

func Test_getContainerCPULimit(t *testing.T) {
	gtx := mockGatherContext()

	// pod
	limit := getContainerCPULimit(gtx)
	assert.Equal(t, 0.5, limit)

	// env
	gtx.podContainer = apiv1.Container{}
	limit = getContainerCPULimit(gtx)
	assert.Equal(t, 0.5, limit)

	// info
	delete(gtx.envs, "DICE_CPU_LIMIT")
	gtx.info.HostConfig.CPUPeriod = 100
	gtx.info.HostConfig.CPUQuota = 20
	limit = getContainerCPULimit(gtx)
	assert.Equal(t, 0.2, limit)

	// no hostConfig
	// none
	gtx.info.HostConfig = nil
	limit = getContainerCPULimit(gtx)
	assert.Equal(t, 0.0, limit)

	// no info
	gtx.info = nil
	limit = getContainerCPULimit(gtx)
	assert.Equal(t, 0.0, limit)
}

func Test_getContainerCPUAllocation(t *testing.T) {
	gtx := mockGatherContext()

	// pod
	alloc := getContainerCPUAllocation(gtx)
	assert.Equal(t, 0.01, alloc)

	// env
	gtx.podContainer = apiv1.Container{}
	alloc = getContainerCPUAllocation(gtx)
	assert.Equal(t, 0.083333, alloc)

	// info
	gtx.info.HostConfig.CPUShares = 512
	delete(gtx.envs, "DICE_CPU_REQUEST")
	alloc = getContainerCPUAllocation(gtx)
	assert.Equal(t, 0.5, alloc)

	// none
	gtx.info.HostConfig = nil
	alloc = getContainerCPUAllocation(gtx)
	assert.Equal(t, 0.0, alloc)

	gtx.info = nil
	alloc = getContainerCPUAllocation(gtx)
	assert.Equal(t, 0.0, alloc)
}

func Test_getContainerCPUOrigin(t *testing.T) {
	gtx := mockGatherContext()

	// env
	origin := getContainerCPUOrigin(gtx)
	assert.Equal(t, 0.5*1024*1024, origin)

	// none
	delete(gtx.envs, "DICE_CPU_ORIGIN")
	origin = getContainerCPUOrigin(gtx)
	assert.Equal(t, 0.0, origin)
}

func TestGatherContainerIO(t *testing.T) {

}

func TestGatherContainerNet(t *testing.T) {

}

func mockStats() *types.StatsJSON {
	stats := &types.StatsJSON{}
	stats.Read = time.Now()
	stats.PreRead = stats.Read.Add(-time.Millisecond * 10)
	stats.NumProcs = 1

	stats.Networks = make(map[string]types.NetworkStats)
	stats.CPUStats.OnlineCPUs = 2
	stats.CPUStats.CPUUsage.PercpuUsage = []uint64{1, 1002, 0, 0}
	stats.CPUStats.CPUUsage.UsageInUsermode = 100
	stats.CPUStats.CPUUsage.TotalUsage = 500
	stats.CPUStats.CPUUsage.UsageInKernelmode = 200
	stats.CPUStats.SystemUsage = 100
	stats.CPUStats.ThrottlingData.Periods = 1

	stats.PreCPUStats.CPUUsage.TotalUsage = 400
	stats.PreCPUStats.SystemUsage = 50

	stats.MemoryStats.Stats = make(map[string]uint64)
	stats.MemoryStats.Stats["active_anon"] = 0
	stats.MemoryStats.Stats["active_file"] = 1
	stats.MemoryStats.Stats["cache"] = 0
	stats.MemoryStats.Stats["hierarchical_memory_limit"] = 0
	stats.MemoryStats.Stats["inactive_anon"] = 0
	stats.MemoryStats.Stats["inactive_file"] = 3
	stats.MemoryStats.Stats["mapped_file"] = 0
	stats.MemoryStats.Stats["pgfault"] = 2
	stats.MemoryStats.Stats["pgmajfault"] = 0
	stats.MemoryStats.Stats["pgpgin"] = 0
	stats.MemoryStats.Stats["pgpgout"] = 0
	stats.MemoryStats.Stats["rss"] = 0
	stats.MemoryStats.Stats["rss_huge"] = 0
	stats.MemoryStats.Stats["total_active_anon"] = 0
	stats.MemoryStats.Stats["total_active_file"] = 0
	stats.MemoryStats.Stats["total_cache"] = 0
	stats.MemoryStats.Stats["total_inactive_anon"] = 0
	stats.MemoryStats.Stats["total_inactive_file"] = 0
	stats.MemoryStats.Stats["total_mapped_file"] = 0
	stats.MemoryStats.Stats["total_pgfault"] = 0
	stats.MemoryStats.Stats["total_pgmajfault"] = 0
	stats.MemoryStats.Stats["total_pgpgin"] = 4
	stats.MemoryStats.Stats["total_pgpgout"] = 0
	stats.MemoryStats.Stats["total_rss"] = 44
	stats.MemoryStats.Stats["total_rss_huge"] = 444
	stats.MemoryStats.Stats["total_unevictable"] = 0
	stats.MemoryStats.Stats["total_writeback"] = 55
	stats.MemoryStats.Stats["unevictable"] = 0
	stats.MemoryStats.Stats["writeback"] = 0

	stats.MemoryStats.MaxUsage = 120 * 1024 * 1024
	stats.MemoryStats.Usage = 100 * 1024 * 1024
	stats.MemoryStats.Failcnt = 1
	stats.MemoryStats.Limit = 512 * 1024 * 1024

	stats.Networks["eth0"] = types.NetworkStats{
		RxDropped: 1,
		RxBytes:   2,
		RxErrors:  3,
		TxPackets: 4,
		TxDropped: 1,
		RxPackets: 2,
		TxErrors:  3,
		TxBytes:   4,
	}

	stats.Networks["eth1"] = types.NetworkStats{
		RxDropped: 5,
		RxBytes:   6,
		RxErrors:  7,
		TxPackets: 8,
		TxDropped: 5,
		RxPackets: 6,
		TxErrors:  7,
		TxBytes:   8,
	}

	sbr := types.BlkioStatEntry{
		Major: 6,
		Minor: 0,
		Op:    "read",
		Value: 100,
	}
	sr := types.BlkioStatEntry{
		Major: 6,
		Minor: 0,
		Op:    "write",
		Value: 101,
	}
	sr2 := types.BlkioStatEntry{
		Major: 6,
		Minor: 1,
		Op:    "write",
		Value: 201,
	}

	stats.BlkioStats.IoServiceBytesRecursive = append(
		stats.BlkioStats.IoServiceBytesRecursive, sbr)
	stats.BlkioStats.IoServicedRecursive = append(
		stats.BlkioStats.IoServicedRecursive, sr)
	stats.BlkioStats.IoServicedRecursive = append(
		stats.BlkioStats.IoServicedRecursive, sr2)

	return stats
}

func mockEnvs() map[string]string {
	return map[string]string{
		"DICE_MEM_ORIGIN":  "512",
		"DICE_MEM_REQUEST": "32",
		"DICE_MEM_LIMIT":   "512",
		"DICE_CPU_LIMIT":   "0.500000",
		"DICE_CPU_REQUEST": "0.083333",
		"DICE_CPU_ORIGIN":  "0.500000",
	}
}

func mockPodContainer() apiv1.Container {
	return apiv1.Container{
		Name: "test-container",
		Resources: apiv1.ResourceRequirements{
			Limits: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("500m"),
				apiv1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Requests: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("10m"),
				apiv1.ResourceMemory: resource.MustParse("10Mi"),
			},
		},
	}
}

func mockInfo() *types.ContainerJSON {
	res := &types.ContainerJSON{
		Config: &container.Config{
			Env: []string{
				"ENVVAR1=loremipsum",
				"ENVVAR1FOO=loremipsum",
				"ENVVAR2=dolorsitamet",
				"ENVVAR3==ubuntu:10.04",
				"ENVVAR4",
				"ENVVAR5=",
				"ENVVAR6= ",
				"ENVVAR7=ENVVAR8=ENVVAR9",
				"PATH=/bin:/sbin",
			},
		},
		ContainerJSONBase: &types.ContainerJSONBase{
			State: &types.ContainerState{
				Health: &types.Health{
					FailingStreak: 1,
					Status:        "Unhealthy",
				},
				Status:     "running",
				OOMKilled:  false,
				Pid:        1234,
				ExitCode:   0,
				StartedAt:  "2018-06-14T05:48:53.266176036Z",
				FinishedAt: "0001-01-01T00:00:00Z",
			},
		},
	}
	res.HostConfig = &container.HostConfig{}
	res.HostConfig.Memory = 268435456
	return res
}

func mockGatherContext() *gatherContext {
	gtx := &gatherContext{}
	gtx.id = "aaa"
	gtx.envs = mockEnvs()
	gtx.podContainer = mockPodContainer()
	gtx.info = mockInfo()
	gtx.stats = mockStats()
	gtx.fields = make(map[string]interface{})
	gtx.tags = make(map[string]string)

	return gtx
}

func mockSummary() *Summary {
	return &Summary{}
}
