package hostsummary

import (
	"os"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
)

// SystemCollector .
type SystemCollector struct{}

// Gather .
func (c *SystemCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	loadavg, err := load.Avg()
	if err != nil && !strings.Contains(err.Error(), "not implemented") {
		return err
	}

	// 系统负载
	if loadavg != nil {
		fields["load1"] = loadavg.Load1
		fields["load5"] = loadavg.Load5
		fields["load15"] = loadavg.Load15
	}

	// 用户数
	users, err := host.Users()
	if err == nil {
		fields["n_users"] = len(users)
	} else if !os.IsPermission(err) {
		return err
	}

	// 启动时间
	uptime, err := host.Uptime()
	if err != nil {
		return err
	}
	fields["uptime"] = uptime
	return nil
}

func init() {
	RegisterCollector("system", func(map[string]interface{}) Collector {
		return &SystemCollector{}
	})
}
