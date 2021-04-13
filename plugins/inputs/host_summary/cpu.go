package hostsummary

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/shirou/gopsutil/cpu"
)

// CPUCollector .
type CPUCollector struct {
	lastStats map[string]cpu.TimesStat
}

// Gather .
func (c *CPUCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	totalTimes, err := cpu.Times(false)
	if err != nil {
		return err
	}
	cpus := runtime.NumCPU()
	fields["n_cpus"] = cpus
	tags["n_cpus"] = strconv.Itoa(cpus)

	if len(c.lastStats) > 0 {
		for _, cts := range totalTimes {
			if cts.CPU != "cpu-total" {
				continue
			}

			total := totalCPUTime(cts)
			active := activeCPUTime(cts)
			lastCts, ok := c.lastStats[cts.CPU]
			if !ok {
				continue
			}
			lastTotal := totalCPUTime(lastCts)
			lastActive := activeCPUTime(lastCts)
			totalDelta := total - lastTotal
			if totalDelta < 0 {
				return fmt.Errorf("current total CPU time is less than previous total CPU time")
			}

			if totalDelta == 0 {
				fields["cpu_usage_active"] = 0
				fields["cpu_cores_usage"] = 0
			} else {
				percent := 100 * (active - lastActive) / totalDelta
				fields["cpu_usage_active"] = percent
				fields["cpu_cores_usage"] = percent / 100 * float64(cpus) // 已经使用的核数
			}
			fields["cpu_usage_user"] = getCPUTime(cts.User, lastCts.User)
			fields["cpu_usage_system"] = getCPUTime(cts.System, lastCts.System)
			fields["cpu_usage_idle"] = getCPUTime(cts.Idle, lastCts.Idle)
			fields["cpu_usage_iowait"] = getCPUTime(cts.Iowait, lastCts.Iowait)
		}
	}

	c.lastStats = make(map[string]cpu.TimesStat)
	for _, cts := range totalTimes {
		c.lastStats[cts.CPU] = cts
	}
	return nil
}

func getCPUTime(cur, last float64) float64 {
	if cur < last {
		return 0
	}
	return cur - last
}

func totalCPUTime(t cpu.TimesStat) float64 {
	total := t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal +
		t.Idle
	return total
}

func activeCPUTime(t cpu.TimesStat) float64 {
	active := totalCPUTime(t) - t.Idle
	return active
}

func init() {
	RegisterCollector("cpu", func(map[string]interface{}) Collector {
		return &CPUCollector{}
	})
}
