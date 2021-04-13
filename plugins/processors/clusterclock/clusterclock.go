package clusterclock

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"github.com/influxdata/telegraf/plugins/processors"
)

type ClusterClock struct {
}

func (b ClusterClock) SampleConfig() string {
	return ""
}

func (b ClusterClock) Description() string {
	return ""
}

func (b ClusterClock) Apply(in ...telegraf.Metric) []telegraf.Metric {
	now := util.Ns2Ms(time.Now().UnixNano())
	ret := make([]telegraf.Metric, 0, len(in))
	for _, m := range in {
		if m.Name() == "cluster_clock" {
			// ignore metric from the same machine
			if tag, _ := m.GetTag("source_host_ip"); tag == node.GetInfo().HostIP() {
				continue
			}

			m.AddField("receive_timestamp", now)
			sendts, ok := m.GetField("send_timestamp")
			if v, ok2 := sendts.(int64); ok && ok2 {
				m.AddField("elapsed_abs", int64(math.Abs(float64(now-v))))
				ret = append(ret, m)
			} else {
				if !ok {
					fmt.Printf("I! get field send_timestamp failed")
				}
				if !ok2 {
					fmt.Printf("I! convert send_timestamp failed. send_timestamp=%s", reflect.TypeOf(sendts).Kind())
				}
			}
		}
	}
	return ret
}

func init() {
	processors.Add("cluster_clock", func() telegraf.Processor {
		p := &ClusterClock{}
		return p
	})
}
