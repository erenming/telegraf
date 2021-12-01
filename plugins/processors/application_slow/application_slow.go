package application_slow

import (
	"log"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

type ApplicationSlow struct {
	SlowHttp    int64 `toml:"slow_http"`
	SlowRpc     int64 `toml:"slow_rpc"`
	SlowCache   int64 `toml:"slow_cache"`
	SlowDB      int64 `toml:"slow_db"`
	SlowDefault int64 `toml:"slow_default"`

	init bool
}

func (d *ApplicationSlow) Description() string {
	return "pick up the slow application"
}

func (d *ApplicationSlow) SampleConfig() string {
	return "slow_default is the default slow time, it effect when other slow time not effect"
}

func (d *ApplicationSlow) Apply(in ...telegraf.Metric) []telegraf.Metric {
	for _, metric := range in {
		idx := strings.Index(metric.Name(), "application_")
		if idx < 0 {
			continue
		}

		value, ok := metric.GetField("elapsed")
		if !ok {
			continue
		}

		elapsed, ok := value.(int64)
		if !ok {
			elapsed = int64(value.(float64))
		}

		// tag中包含error，并且值为true，创建application_*_error的 metric，并且保留request_id
		if errorVal, ok := metric.GetTag("error"); ok {
			if e, err := strconv.ParseBool(errorVal); err == nil && e {
				errMetric := metric.Copy()
				errMetric.SetName(metric.Name() + "_error")
				in = append(in, errMetric)
			}
		}

		var slowLimit int64
		switch metric.Name()[idx+12:] {
		case "http":
			slowLimit = d.SlowHttp
		case "rpc":
			slowLimit = d.SlowRpc
		case "cache":
			slowLimit = d.SlowCache
		case "db":
			slowLimit = d.SlowDB
		default:
			slowLimit = d.SlowDefault
		}

		if elapsed > slowLimit {
			slowMetric := metric.Copy()
			slowMetric.SetName(metric.Name() + "_slow")

			if sampledTag, ok := slowMetric.GetTag("trace_sampled"); ok {
				if sampled, err := strconv.ParseBool(sampledTag); err != nil {
					log.Printf("trace_sampled %s is not bool", sampledTag)
					slowMetric.RemoveTag("request_id")
				} else if !sampled {
					slowMetric.RemoveTag("request_id")
				}
			} else {
				slowMetric.RemoveTag("request_id")
			}
			in = append(in, slowMetric)
		}

		//移除request_id，使metric可以聚合
		if metric.HasTag("request_id") {
			metric.RemoveTag("request_id")
		}

		if metric.HasTag("trace_sampled") {
			metric.RemoveTag("trace_sampled")
		}
	}
	return in
}

func (d *ApplicationSlow) initOnce() {
	if d.init {
		return
	}
	defer func() {
		d.init = true
	}()

	if d.SlowHttp == 0 {
		d.SlowHttp = d.SlowDefault
	}
	if d.SlowRpc == 0 {
		d.SlowRpc = d.SlowDefault
	}
	if d.SlowCache == 0 {
		d.SlowCache = d.SlowDefault
	}
	if d.SlowDB == 0 {
		d.SlowDB = d.SlowDefault
	}
}

func init() {
	processors.Add("application_slow", func() telegraf.Processor {
		p := &ApplicationSlow{
			SlowCache:   50000000,
			SlowDB:      100000000,
			SlowDefault: 300000000,
		}
		return p
	})
}
