package hostsummary

import (
	"fmt"
	"log"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
)

// Summary .
type Summary struct {
	GathersEnable []string               `toml:"gathers"`
	Config        map[string]interface{} `toml:"config"`
	gathers       map[string]Collector
}

// Description .
func (*Summary) Description() string { return "" }

// SampleConfig .
func (*Summary) SampleConfig() string { return "" }

// Gather .
func (s *Summary) Gather(acc telegraf.Accumulator) (err error) {
	if len(s.gathers) <= 0 {
		return nil
	}
	tags := make(map[string]string)
	fields := make(map[string]interface{})
	for name, c := range s.gathers {
		err := c.Gather(tags, fields, acc)
		if err != nil {
			log.Printf("E! [host_summary] fail to gather %s", name)
			continue
		}
	}

	if request, ok := fields["mem_request_total"]; ok {
		request := toFloat64(request, 0)
		total := node.GetInfo().AllocatableMEM()
		if total == 0 {
			fields["mem_request_percent"] = 0
		} else {
			fields["mem_request_percent"] = request * 100 / total
			fields["mem_allocatable"] = total
		}
	}
	if request, ok := fields["cpu_request_total"]; ok {
		request := toFloat64(request, 0)
		total := node.GetInfo().AllocatableCPU()
		if total == 0 {
			fields["cpu_request_percent"] = 0
		} else {
			fields["cpu_request_percent"] = request * 100 / total
			fields["cpu_allocatable"] = total
		}
	}
	acc.AddFields("host_summary", fields, tags)
	return nil
}

// Start .
func (s *Summary) Start(acc telegraf.Accumulator) error {
	if len(s.GathersEnable) <= 0 {
		return nil
	}
	s.gathers = make(map[string]Collector)
	for _, name := range s.GathersEnable {
		creator, ok := gathers[name]
		if !ok {
			return fmt.Errorf("[host_summary] gather %s not exist", name)
		}
		s.gathers[name] = creator(s.Config)
	}
	for name, c := range s.gathers {
		if s, ok := c.(telegraf.ServiceInput); ok {
			err := s.Start(acc)
			if err != nil {
				return fmt.Errorf("[host_summary] Service for gather %s failed to start: %v", name, err)
			}
		}
	}
	return nil
}

// Stop .
func (s *Summary) Stop() {
	if len(s.gathers) <= 0 {
		return
	}
	for _, c := range s.gathers {
		if s, ok := c.(telegraf.ServiceInput); ok {
			s.Stop()
		}
	}
}

// Collector .
type Collector interface {
	Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error
}

var gathers = map[string]func(cfg map[string]interface{}) Collector{}

// RegisterCollector .
func RegisterCollector(name string, creator func(cfg map[string]interface{}) Collector) {
	gathers[name] = creator
}

func init() {
	inputs.Add("host_summary", func() telegraf.Input {
		return &Summary{}
	})
}

func toFloat64(obj interface{}, defVal float64) float64 {
	switch val := obj.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return float64(val)
	case string:
		v, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return v
		}
	}
	return defVal
}
