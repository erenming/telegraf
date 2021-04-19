package point_space_to_underline

import (
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

const sampleConfig = ``

type PointAndSpaceToUnderline struct{}

func (p *PointAndSpaceToUnderline) Description() string {
	return "Replace point to underline."
}

func (p *PointAndSpaceToUnderline) SampleConfig() string {
	return sampleConfig
}

func (p *PointAndSpaceToUnderline) Apply(in ...telegraf.Metric) []telegraf.Metric {
	for _, metric := range in {
		fields := metric.Fields()
		for k, v := range fields {
			metric.RemoveField(k)
			k = strings.Replace(k, ".", "_", -1)
			k = strings.Replace(k, " ", "_", -1)
			metric.AddField(k, v)
		}
		tags := metric.Tags()
		for k, v := range tags {
			metric.RemoveTag(k)
			k = strings.Replace(k, ".", "_", -1)
			k = strings.Replace(k, " ", "_", -1)
			metric.AddTag(k, v)
		}
	}
	return in
}

func init() {
	processors.Add("point_space_to_underline", func() telegraf.Processor {
		return &PointAndSpaceToUnderline{}
	})
}
