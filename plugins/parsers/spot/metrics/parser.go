package metrics

import (
	"encoding/json"
	"time"

	"github.com/influxdata/telegraf"
	metric2 "github.com/influxdata/telegraf/metric"
)

type MetricsParser struct {
}

func (p *MetricsParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	var data metrics
	if err := json.Unmarshal(buf, &data); err != nil {
		return nil, err
	}
	metrics := make([]telegraf.Metric, 0)
	for _, d := range data {
		r, err := metric2.New(d.Name, d.Tags, d.Fields, time.Unix(0, d.Timestamp), telegraf.Untyped)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, r)
	}
	return metrics, nil
}
