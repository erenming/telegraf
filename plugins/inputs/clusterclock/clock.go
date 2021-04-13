package clusterclock

import (
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
)

type ClusterClock struct {
}

func (c ClusterClock) SampleConfig() string {
	return ""
}

func (c ClusterClock) Description() string {
	return ""
}

func (c ClusterClock) Gather(accumulator telegraf.Accumulator) error {
	accumulator.AddFields("cluster_clock", map[string]interface{}{"send_timestamp": util.Ns2Ms(time.Now().UnixNano())}, map[string]string{
		"source_host": node.GetInfo().Hostname(),
		"source_host_ip": node.GetInfo().HostIP(),
	})
	return nil
}

func init() {
	inputs.Add("cluster_clock", func() telegraf.Input {
		return &ClusterClock{}
	})
}
