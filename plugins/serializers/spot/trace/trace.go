package trace

import (
	"bytes"

	"github.com/influxdata/telegraf"
)

var (
	empty = []byte("")
)

type serializer struct {
}

func (s *serializer) Serialize(metric telegraf.Metric) ([]byte, error) {
	if metric.Name() == "trace" {
		if content, ok := metric.GetTag("content"); ok {
			buffer := new(bytes.Buffer)
			buffer.Write([]byte("{\"trace\":["))
			buffer.Write([]byte(content))
			buffer.Write([]byte("]}"))
			return buffer.Bytes(), nil
		}
	}
	return empty, nil
}

func (s *serializer) SerializeBatch(metrics []telegraf.Metric) ([]byte, error) {
	traces := make([]telegraf.Metric, 0)
	for _, m := range metrics {
		if m.Name() == "trace" {
			traces = append(traces, m)
		}
	}
	if len(traces) == 0 {
		return empty, nil
	}
	buffer := new(bytes.Buffer)
	buffer.Write([]byte("{\"trace\":["))
	for idx, m := range traces {
		if content, ok := m.GetTag("content"); ok {
			buffer.Write([]byte(content))
			if idx != len(traces)-1 {
				buffer.WriteByte(',')
			}
		}
	}

	buffer.Write([]byte("]}"))
	return buffer.Bytes(), nil
}

func NewSerializer() (*serializer, error) {
	return &serializer{}, nil
}
