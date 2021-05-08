package summary

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

type Summary struct {
	Name        string   `toml:"name"`
	Fields      []string `toml:"fields"`
	TypeTagKey  string   `toml:"type_tag_key"`
	Types       []string `toml:"types"`
	DistinctTag string   `toml:"distinct_tag"`
	cache       map[string]map[string]map[string]map[string]float64
}

func NewSummary() *Summary {
	s := &Summary{}
	s.Reset()
	return s
}

var sampleConfig = ``

func (*Summary) SampleConfig() string {
	return sampleConfig
}

func (*Summary) Description() string {
	return ""
}

func (s *Summary) Add(in telegraf.Metric) {
	key, ok := in.GetTag(s.TypeTagKey)
	if !ok {
		return
	}
	if key == "" {
		key = "none"
	}
	var typeInfo map[string]map[string]map[string]float64
	if typeInfo, ok = s.cache[key]; !ok {
		typeInfo = make(map[string]map[string]map[string]float64)
		s.cache[key] = typeInfo
	}
	orgName, _ := in.GetTag("org_name")
	if orgName == "" {
		orgName = "none"
	}
	var orgInfo map[string]map[string]float64
	if orgInfo, ok = typeInfo[orgName]; !ok {
		orgInfo = make(map[string]map[string]float64)
		typeInfo[orgName] = orgInfo
	}
	if id, ok := in.GetTag(s.DistinctTag); ok {
		var fields map[string]float64
		if fields, ok = orgInfo[id]; !ok {
			fields = make(map[string]float64)
			orgInfo[id] = fields
		}
		for _, field := range s.Fields {
			if val, ok := in.GetField(field); ok {
				if v, ok := convert(val); ok {
					fields[field] = v
				}
			} else {
				fields[field] = 0
			}
		}
	}
}

func (s *Summary) Push(acc telegraf.Accumulator) {
	fields := map[string]interface{}{}
	if len(s.cache) <= 0 {
		return
	}
	orgs := make(map[string]bool)
	for typeName, tinfo := range s.cache {
		for orgName, oinfo := range tinfo {
			for _, dinfo := range oinfo {
				aggFields(orgName+"_", typeName, fields, dinfo)
				aggFields("", typeName, fields, dinfo)
				orgs[orgName] = true
			}
		}
	}
	for _, typeName := range s.Types {
		for _, field := range s.Fields {
			key := typeName + "_" + field
			if _, ok := fields[key]; !ok {
				fields[key] = 0
			}
			for org := range orgs {
				key = org + "_" + typeName + "_" + field
				if _, ok := fields[key]; !ok {
					fields[key] = 0
				}
			}
		}
		if _, ok := fields[typeName]; !ok {
			fields[typeName] = 0
		}
		for org := range orgs {
			key := org + "_" + typeName
			if _, ok := fields[key]; !ok {
				fields[key] = 0
			}
		}
	}
	acc.AddFields(s.Name, fields, map[string]string{})
}

func aggFields(prefix, typeName string, fields map[string]interface{}, dinfo map[string]float64) {
	prefix = prefix + typeName
	for field, val := range dinfo {
		key := prefix + "_" + field
		if v, ok := fields[key]; ok {
			if _v, ok := v.(float64); ok {
				fields[key] = _v + val
			}
		} else {
			fields[key] = val
		}
	}
	if v, ok := fields[prefix]; ok {
		if _v, ok := v.(int); ok {
			fields[prefix] = _v + 1
		}
	} else {
		fields[prefix] = int(1)
	}
}

func (s *Summary) Reset() {
	s.cache = make(map[string]map[string]map[string]map[string]float64)
}

func convert(in interface{}) (float64, bool) {
	switch v := in.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func init() {
	aggregators.Add("summary", func() telegraf.Aggregator {
		return NewSummary()
	})
}
