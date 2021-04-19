package hostsummary

import (
	"fmt"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"github.com/shirou/gopsutil/host"
)

// NodeCollector .
type NodeCollector struct{}

// Gather .
func (c *NodeCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	info := node.GetInfo()
	tags["hostname"] = info.Hostname()
	platform, _, version, err := host.PlatformInformation()
	if err != nil {
		return fmt.Errorf("fail to get platform info: %s", err)
	}
	kernelVersion, err := host.KernelVersion()
	if err != nil {
		return fmt.Errorf("fail to get kernel version: %s", err)
	}
	labels, err := node.GetLabels()
	if err == nil {
		fields["labels"] = labels.List()
	}
	tags["os"] = platform + " " + version
	tags["kernel_version"] = kernelVersion
	tags["labels"] = strings.Join(labels.List(), ",")
	return nil
}

func init() {
	RegisterCollector("node", func(map[string]interface{}) Collector {
		return &NodeCollector{}
	})
}
