package spot

import (
	"encoding/json"
	"github.com/influxdata/telegraf"
	metric2 "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers/spot/metrics"
	"time"
)

type SingleMetricParser struct {
	DefaultTags map[string]string
}

func (p *SingleMetricParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	var data metrics.Metric
	if err := json.Unmarshal(buf, &data); err != nil {
		return nil, err
	}
	r, err := metric2.New(data.Name, data.Tags, data.Fields, time.Unix(0, data.Timestamp), telegraf.Untyped)
	if err != nil {
		return nil, err
	}
	return []telegraf.Metric{r}, nil
}

func (p *SingleMetricParser) ParseLine(line string) (telegraf.Metric, error) {
	data, err := p.Parse([]byte(line))
	if err != nil {
		return nil, err
	}
	return data[0], nil
}

func (p *SingleMetricParser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}

func (p *SingleMetricParser) applyDefaultTags(metrics []telegraf.Metric) {
	if len(p.DefaultTags) == 0 {
		return
	}

	for _, m := range metrics {
		for k, v := range p.DefaultTags {
			if !m.HasTag(k) {
				m.AddTag(k, v)
			}
		}
	}
}
