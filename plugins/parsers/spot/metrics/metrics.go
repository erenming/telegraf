package metrics

type Metric struct {
	Name      string                 `json:"name,omitempty"`
	Timestamp int64                  `json:"timestamp,omitempty"`
	Tags      map[string]string      `json:"tags,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

type metrics []Metric
