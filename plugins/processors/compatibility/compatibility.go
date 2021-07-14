package compatibility

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"github.com/influxdata/telegraf/plugins/processors"
)

// Compatibility 兼容老的版本的dice指标
type Compatibility struct{}

// Description .
func (*Compatibility) Description() string { return "" }

// SampleConfig .
func (*Compatibility) SampleConfig() string { return "" }

// Apply .
func (c *Compatibility) Apply(in ...telegraf.Metric) []telegraf.Metric {
	var out []telegraf.Metric
	for _, m := range in {
		out = append(out, m)
		if m.Name() == "host_summary" {
			out = append(out, c.convertToMachineSummary(m))
		} else if m.Name() == "docker_container_summary" {
			splits := c.splitDockerMetrics(m)
			out = append(out, splits...)
			// 过滤 k8s pause 容器
			if _, ok := m.GetTag("podsandbox"); !ok {
				out = append(out, c.convertToContainerSummary(m))
			}
		}
	}
	return out
}

func (c *Compatibility) splitHostMetrics(m telegraf.Metric) []telegraf.Metric {
	var out []telegraf.Metric
	// mem
	// tags, fields := make(map[string]string), make(map[string]interface{})
	// if val, ok := m.GetField("mem_total"); ok {
	// 	fields["total"] = val
	// }
	// if val, ok := m.GetField("mem_used"); ok {
	// 	fields["used"] = val
	// }
	// if val, ok := m.GetField("mem_used_percent"); ok {
	// 	fields["used_percent"] = val
	// }
	// if val, ok := m.GetField("mem_free"); ok {
	// 	fields["free"] = val
	// }
	// if val, ok := m.GetField("mem_available"); ok {
	// 	fields["available"] = val
	// }
	// if val, ok := m.GetField("mem_available_percent"); ok {
	// 	fields["available_percent"] = val
	// }
	// if val, ok := m.GetField("swap_free"); ok {
	// 	fields["swap_free"] = val
	// }
	// if val, ok := m.GetField("swap_total"); ok {
	// 	fields["swap_total"] = val
	// }
	// mem, _ := metric.New("mem", tags, fields, m.Time())
	// out = append(out, mem)

	// swap
	// tags, fields = make(map[string]string), make(map[string]interface{})
	// var swapFree, swapTotal uint64
	// if val, ok := m.GetField("swap_free"); ok {
	// 	fields["free"] = val
	// 	swapFree = toUint64(val, 0)
	// }
	// if val, ok := m.GetField("swap_total"); ok {
	// 	fields["total"] = val
	// 	swapTotal = toUint64(val, 0)
	// }
	// fields["used"] = swapTotal - swapFree
	// if swapTotal == 0 {
	// 	fields["used_percent"] = 0
	// } else {
	// 	fields["used_percent"] = 100 * float64(swapTotal-swapFree) / float64(swapTotal)
	// }
	// swap, _ := metric.New("swap", tags, fields, m.Time())
	// out = append(out, swap)

	// cpu
	// tags, fields = make(map[string]string), make(map[string]interface{})
	// tags["cpu"] = "cpu-total"
	// if val, ok := m.GetField("cpu_usage_active"); ok {
	// 	fields["usage_active"] = val
	// }
	// cpu, _ := metric.New("cpu", tags, fields, m.Time())
	// out = append(out, cpu)

	// system
	// tags, fields = make(map[string]string), make(map[string]interface{})
	// if val, ok := m.GetField("load1"); ok {
	// 	fields["load1"] = val
	// }
	// if val, ok := m.GetField("load5"); ok {
	// 	fields["load5"] = val
	// }
	// if val, ok := m.GetField("load15"); ok {
	// 	fields["load15"] = val
	// }
	// if val, ok := m.GetField("n_cpus"); ok {
	// 	fields["n_cpus"] = val
	// }
	// if val, ok := m.GetField("n_users"); ok {
	// 	fields["n_users"] = val
	// }
	// if val, ok := m.GetField("uptime"); ok {
	// 	fields["uptime"] = val
	// 	fields["uptime_format"] = formatUptime(toUint64(val, 0))
	// }
	// system, _ := metric.New("system", tags, fields, m.Time())
	// out = append(out, system)

	return out
}

func (c *Compatibility) splitDockerMetrics(m telegraf.Metric) []telegraf.Metric {
	var out []telegraf.Metric
	tags := m.Tags()
	// mem
	// fields := make(map[string]interface{})
	// if val, ok := m.GetField("mem_allocation"); ok {
	// 	fields["allocation"] = val
	// }
	// if val, ok := m.GetField("mem_limit"); ok {
	// 	fields["limit"] = val
	// }
	// if val, ok := m.GetField("mem_max_usage"); ok {
	// 	fields["max_usage"] = val
	// }
	// if val, ok := m.GetField("mem_usage"); ok {
	// 	fields["usage"] = val
	// }
	// if val, ok := m.GetField("mem_usage_percent"); ok {
	// 	fields["usage_percent"] = val
	// }
	// if val, ok := m.GetField("mem_commit_bytes"); ok {
	// 	fields["commit_bytes"] = val
	// }
	// if val, ok := m.GetField("mem_commit_peak_bytes"); ok {
	// 	fields["commit_peak_bytes"] = val
	// }
	// if val, ok := m.GetField("mem_private_working_set"); ok {
	// 	fields["private_working_set"] = val
	// }
	// if len(fields) > 0 {
	// 	mem, _ := metric.New("docker_container_mem", tags, fields, m.Time())
	// 	out = append(out, mem)
	// }

	// cpu
	// cpuTags := m.Tags()
	// cpuTags["cpu"] = "cpu-total"
	// fields = make(map[string]interface{})
	// if val, ok := m.GetField("cpu_limit"); ok {
	// 	fields["limit"] = val
	// }
	// if val, ok := m.GetField("cpu_allocation"); ok {
	// 	fields["allocation"] = val
	// }
	// if val, ok := m.GetField("cpu_usage_percent"); ok {
	// 	fields["usage_percent"] = val
	// }
	// if len(fields) > 0 {
	// 	cpu, _ := metric.New("docker_container_cpu", cpuTags, fields, m.Time())
	// 	out = append(out, cpu)
	// }

	// blk io
	// blkTags := m.Tags()
	// blkTags["device"] = "total"
	// fields = make(map[string]interface{})
	// if val, ok := m.GetField("blk_read_bytes"); ok {
	// 	fields["io_service_bytes_recursive_read"] = val
	// }
	// if val, ok := m.GetField("blk_write_bytes"); ok {
	// 	fields["io_service_bytes_recursive_write"] = val
	// }
	// if val, ok := m.GetField("blk_sync_bytes"); ok {
	// 	fields["io_service_bytes_recursive_sync"] = val
	// }
	// if val, ok := m.GetField("blk_reads"); ok {
	// 	fields["io_serviced_recursive_read"] = val
	// }
	// if val, ok := m.GetField("blk_writes"); ok {
	// 	fields["io_serviced_recursive_write"] = val
	// }
	// if val, ok := m.GetField("blk_syncs"); ok {
	// 	fields["io_serviced_recursive_sync"] = val
	// }
	// if len(fields) > 0 {
	// 	blkio, _ := metric.New("docker_container_blkio", blkTags, fields, m.Time())
	// 	out = append(out, blkio)
	// }

	// net
	// netTags := m.Tags()
	// netTags["network"] = "total"
	// fields = make(map[string]interface{})
	// if val, ok := m.GetField("rx_dropped"); ok {
	// 	fields["rx_dropped"] = val
	// }
	// if val, ok := m.GetField("rx_bytes"); ok {
	// 	fields["rx_bytes"] = val
	// }
	// if val, ok := m.GetField("rx_errors"); ok {
	// 	fields["rx_errors"] = val
	// }
	// if val, ok := m.GetField("rx_packets"); ok {
	// 	fields["rx_packets"] = val
	// }
	// if val, ok := m.GetField("tx_dropped"); ok {
	// 	fields["tx_dropped"] = val
	// }
	// if val, ok := m.GetField("tx_bytes"); ok {
	// 	fields["tx_bytes"] = val
	// }
	// if val, ok := m.GetField("tx_errors"); ok {
	// 	fields["tx_errors"] = val
	// }
	// if val, ok := m.GetField("tx_packets"); ok {
	// 	fields["tx_packets"] = val
	// }
	// if len(fields) > 0 {
	// 	net, _ := metric.New("docker_container_net", netTags, fields, m.Time())
	// 	out = append(out, net)
	// }

	// status
	// todo 仍有业务使用，暂时保留
	fields := make(map[string]interface{})
	if val, ok := m.GetField("oomkilled"); ok {
		fields["oomkilled"] = val
	}
	if val, ok := m.GetField("pid"); ok {
		fields["pid"] = val
	}
	if val, ok := m.GetField("exitcode"); ok {
		fields["exitcode"] = val
	}
	if val, ok := m.GetField("started_at"); ok {
		fields["started_at"] = val
	}
	if val, ok := m.GetField("finished_at"); ok {
		fields["finished_at"] = val
	}
	if len(fields) > 0 {
		status, _ := metric.New("docker_container_status", tags, fields, m.Time())
		out = append(out, status)
	}
	return out
}

// convertToMachineSummary 给企业后台使用
func (c *Compatibility) convertToMachineSummary(m telegraf.Metric) telegraf.Metric {
	tags := m.Tags()
	info := node.GetInfo()
	tags["terminus_index_id"] = info.ClusterName() + "/" + info.HostIP() + "/2"
	tags["terminus_version"] = "2"
	labels, err := node.GetLabels()
	if err == nil {
		tags["labels"] = strings.Join(labels.List(), ",")
	} else {
		tags["labels"] = ""
	}

	fields := make(map[string]interface{})
	if val, ok := m.GetField("labels"); ok {
		fields["labels"] = val
	}
	// load
	if val, ok := m.GetField("load1"); ok {
		fields["load1"] = val
	}
	if val, ok := m.GetField("load5"); ok {
		fields["load5"] = val
		if cpus, ok := m.GetField("n_cpus"); ok {
			cpus := toFloat64(cpus, 0)
			load5 := toFloat64(val, 0)
			if cpus != 0 {
				fields["load_percent"] = load5 * 100 / cpus
			} else {
				fields["load_percent"] = 0
			}
		}
	}
	if val, ok := m.GetField("load15"); ok {
		fields["load15"] = val
	}

	// cpu
	if val, ok := m.GetField("n_cpus"); ok {
		tags["cpus"] = fmt.Sprint(toUint64(val, 0))
		fields["cpu_total"] = val
	}
	if val, ok := m.GetField("cpu_limit_total"); ok {
		fields["cpu_limit"] = val
	}
	if val, ok := m.GetField("cpu_request_total"); ok {
		fields["cpu_request"] = val
	}
	if val, ok := m.GetField("cpu_origin_total"); ok {
		fields["cpu_origin"] = val
	}
	if val, ok := m.GetField("cpu_request_percent"); ok {
		fields["cpu_disp_percent"] = val
	}
	if val, ok := m.GetField("cpu_allocatable"); ok {
		fields["cpu_allocatable"] = val
	}
	if val, ok := m.GetField("cpu_usage_active"); ok {
		fields["cpu_usage_active"] = val
		fields["cpu_usage_percent"] = val
	}
	if val, ok := m.GetField("cpu_cores_usage"); ok {
		fields["cpu_usage"] = val
	}

	// mem
	if val, ok := m.GetField("mem_used"); ok {
		fields["mem_usage"] = val
	}
	if val, ok := m.GetField("mem_used_percent"); ok {
		fields["mem_usage_percent"] = val
	}
	if val, ok := m.GetField("mem_total"); ok {
		fields["mem_total"] = val
	}
	if val, ok := m.GetField("mem_limit_total"); ok {
		fields["mem_limit"] = val
	}
	if val, ok := m.GetField("mem_allocatable"); ok {
		fields["mem_allocatable"] = val
	}
	if val, ok := m.GetField("mem_request_total"); ok {
		fields["mem_request"] = val
	}
	if val, ok := m.GetField("mem_origin_total"); ok {
		fields["mem_origin"] = val
	}
	if val, ok := m.GetField("mem_request_percent"); ok {
		fields["mem_disp_percent"] = val
	}

	// disk
	if val, ok := m.GetField("disk_total"); ok {
		fields["disk_total"] = val
	}
	if val, ok := m.GetField("disk_used"); ok {
		fields["disk_usage"] = val
	}
	if val, ok := m.GetField("disk_used_percent"); ok {
		fields["disk_usage_percent"] = val
	}

	// system
	if val, ok := m.GetField("kernel_version"); ok {
		fields["kernel_version"] = val
	}
	if val, ok := m.GetField("os"); ok {
		fields["os"] = val
	}
	if val, ok := m.GetField("task_containers"); ok {
		fields["tasks"] = val
	}
	m, _ = metric.New("machine_summary", tags, fields, m.Time())
	return m
}

// convertToContainerSummary 兼容老的 container_summary
func (c *Compatibility) convertToContainerSummary(m telegraf.Metric) telegraf.Metric {
	tags := m.Tags()
	tags["terminus_version"] = "2"

	fields := make(map[string]interface{})
	var image string
	if val, ok := m.GetTag("container_image"); ok {
		image = val
	}
	if val, ok := m.GetTag("image_version"); ok {
		image = fmt.Sprintf("%s:%s", image, val)
	}
	fields["image"] = image

	// cpu
	if val, ok := m.GetField("cpu_limit"); ok {
		fields["cpu_limit"] = val
	}
	if val, ok := m.GetField("cpu_origin"); ok {
		fields["cpu_origin"] = val
	}
	if val, ok := m.GetField("cpu_allocation"); ok {
		fields["cpu_request"] = val
	}
	if val, ok := m.GetField("cpu_usage_percent"); ok {
		fields["cpu_usage"] = toFloat64(val, 0) / 100
	}

	// mem
	if val, ok := m.GetField("mem_limit"); ok {
		fields["mem_limit"] = val
		fields["mem_total"] = val
	}
	if val, ok := m.GetField("mem_origin"); ok {
		fields["mem_origin"] = val
	}
	if val, ok := m.GetField("mem_allocation"); ok {
		fields["mem_request"] = val
	}
	if val, ok := m.GetField("mem_usage"); ok {
		fields["mem_usage"] = val
	}
	m, _ = metric.New("container_summary", tags, fields, m.Time())
	return m
}

func formatUptime(uptime uint64) string {
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	days := uptime / (60 * 60 * 24)

	if days != 0 {
		s := ""
		if days > 1 {
			s = "s"
		}
		fmt.Fprintf(w, "%d day%s, ", days, s)
	}

	minutes := uptime / 60
	hours := minutes / 60
	hours %= 24
	minutes %= 60

	fmt.Fprintf(w, "%2d:%02d", hours, minutes)

	w.Flush()
	return buf.String()
}

func toUint64(obj interface{}, defVal uint64) uint64 {
	switch val := obj.(type) {
	case int:
		return uint64(val)
	case int8:
		return uint64(val)
	case int16:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return uint64(val)
	case float32:
		return uint64(val)
	case float64:
		return uint64(val)
	case string:
		v, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			return v
		}
	}
	return defVal
}

func toFloat64(obj interface{}, defVal float64) float64 {
	switch val := obj.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return float64(val)
	case string:
		v, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return v
		}
	}
	return defVal
}

func init() {
	processors.Add("compatibility", func() telegraf.Processor {
		return &Compatibility{}
	})
}
