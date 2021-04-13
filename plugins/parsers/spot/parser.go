package spot

import (
	"errors"
	"fmt"

	"github.com/influxdata/telegraf"

	"github.com/influxdata/telegraf/plugins/parsers/spot/metrics"
)

var (
	traceType, metricType byte = '0', '1'
)

type SpotParser struct {
	DefaultTags map[string]string
	parser      parser
}

type parser interface {
	Parse(buf []byte) ([]telegraf.Metric, error)
}

func (p *SpotParser) InitParsers() {
	//p.parsers = make(map[byte]parser)
	//p.parsers[traceType] = &trace.TraceParser{}
	//p.parsers[metricType] = &metrics.MetricsParser{}
	p.parser = &metrics.MetricsParser{}
}

func (p *SpotParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	if len(buf) <= 1 {
		return nil, errors.New("invalid data.")
	}

	result, err := p.parser.Parse(buf)
	if err != nil {
		return nil, err
	}

	p.applyDefaultTags(result)
	return result, nil
}

func (p *SpotParser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line))

	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, fmt.Errorf("can not parse the line: %s, for data format: trace ", line)
	}

	return metrics[0], nil
}

func (p *SpotParser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}

func (p *SpotParser) applyDefaultTags(metrics []telegraf.Metric) {
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
