package trace

import (
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

type TraceParser struct {
}

func (p *TraceParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	fields := make(map[string]interface{})
	fields["field"] = 0
	tags := make(map[string]string)
	tags["content"] = string(buf)

	r, err := metric.New("trace", tags, fields, time.Now(), telegraf.Untyped)
	if err != nil {
		return nil, err
	}

	metrics := make([]telegraf.Metric, 0)
	metrics = append(metrics, r)
	return metrics, nil
}
