package meta

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

var (
	metaTag    = "_meta"
	customTag  = "_custom"
	scopeTag   = "_metric_scope"
	scopeIdTag = "_metric_scope_id"
)

type Meta struct {
	Scope       string   `toml:"scope"`
	ScopeIDKeys []string `toml:"scope_id_keys"`
	Override    bool     `toml:"override"`
}

func (*Meta) Description() string {
	return "set meta tags"
}

func (*Meta) SampleConfig() string {
	return "set meta tags"
}

func (m *Meta) Apply(in ...telegraf.Metric) []telegraf.Metric {
	for _, metric := range in {
		if metric.HasTag(metaTag) {
			if !m.Override {
				continue
			}
		} else {
			metric.AddTag(metaTag, "true")
		}
		if metric.HasTag(scopeTag) {
			metric.RemoveTag(scopeTag)
		}
		metric.AddTag(scopeTag, m.Scope)

		if metric.HasTag(scopeIdTag) {
			metric.RemoveTag(scopeIdTag)
		}
		for _, key := range m.ScopeIDKeys {
			if val, ok := metric.GetTag(key); ok {
				if metric.HasTag(scopeIdTag) {
					metric.RemoveTag(scopeIdTag)
				}
				metric.AddTag(scopeIdTag, val)
				break
			}
		}
	}
	return in
}

func init() {
	processors.Add("meta", func() telegraf.Processor {
		return &Meta{Override: false, ScopeIDKeys: []string{}}
	})
}
