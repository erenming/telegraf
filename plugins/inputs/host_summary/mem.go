package hostsummary

import (
	"fmt"
	"math"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

// MemCollector .
type MemCollector struct {
	lastStats map[string]cpu.TimesStat
}

// Gather .
func (c *MemCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("error getting virtual memory info: %s", err)
	}
	fields["mem_used"] = vm.Used
	fields["mem_used_percent"] = 100 * float64(vm.Used) / float64(vm.Total)
	fields["mem_available_percent"] = 100 * float64(vm.Available) / float64(vm.Total)
	fields["mem_free"] = vm.Free
	fields["mem_available"] = vm.Available
	fields["mem_total"] = vm.Total

	fields["swap_total"] = vm.SwapTotal
	fields["swap_free"] = vm.SwapFree
	fields["swap_used"] = vm.SwapTotal - vm.SwapFree
	if vm.SwapTotal > 0 {
		fields["swap_used_percent"] = 100 * float64((vm.SwapTotal-vm.SwapFree)/vm.SwapTotal)
	} else {
		fields["swap_used_percent"] = 0
	}

	tags["mem"] = formatMem(vm.Total)
	return nil
}

// 内存规格取整数
func formatMem(total uint64) string {
	mem := math.Round(float64(total) / 1024 / 1024 / 1024)
	return strconv.FormatUint(uint64(mem), 10)
}

func init() {
	RegisterCollector("mem", func(map[string]interface{}) Collector {
		return &MemCollector{}
	})
}
