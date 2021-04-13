package trace

type Span struct {
	SpanId                string          `json:"spanId,omitempty"`
	TraceId               string          `json:"traceId,omitempty"`
	ParentSpanId          string          `json:"parentSpanId,omitempty"`
	Sampled               bool            `json:"sampled,omitempty"`
	OperationName         string          `json:"operationName,omitempty"`
	StartTime             int64           `json:"startTime,omitempty"`
	EndTime               int64           `json:"endTime,omitempty"`
	Tags                  []*KeyValuePair `json:"tags,omitempty"`
	Logs                  []*SpanLog      `json:"logs,omitempty"`
	Baggage               []*KeyValuePair `json:"baggage,omitempty"`
	InstanceId            string          `json:"instanceId,omitempty"`
	ParentSpanTerminusApp string          `json:"parentSpanTerminusApp,omitempty"`
	ParentSpanComponent   string          `json:"parentSpanComponent,omitempty"`
}

type SpanLog struct {
	TimeStamp int64           `json:"timeStamp,omitempty"`
	LogField  []*KeyValuePair `json:"logField,omitempty"`
}

type KeyValuePair struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}