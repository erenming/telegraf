package selfstat

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WriteOutputErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "telegraf_write_output_error_total",
	})
	lastWriteErrorCounterValue = 0
)

type Config struct {
}

type manager struct {
	Enable bool `toml:"-"`
	// the exported http port
	Port int `toml:"port"`

	PrometheusPath   string
	HealthStatusPath string
}

func (m *manager) Start(accumulator telegraf.Accumulator) error {
	http.HandleFunc(m.HealthStatusPath, healthStatus)
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", m.Port), nil)
		if err != nil {
			accumulator.AddError(err)
		}
	}()
	return nil
}

func (m *manager) Stop() {}

func (m *manager) SampleConfig() string {
	return ""
}

func (m *manager) Description() string {
	return ""
}

func (m *manager) Gather(_ telegraf.Accumulator) error {
	return nil
}

type healthStatusDTO struct {
	Status  status       `json:"status"`
	Modules []*subModule `json:"modules"`
}

type subModule struct {
	Name    string `json:"name"`
	Status  status `json:"status"`
	Message string `json:"message"`
}

type checkFunc func() (string, status)
type status string

const (
	ok      = status("ok")
	warning = status("warning")
	fail    = status("fail")
)

var checkMap = map[string]checkFunc{
}

func healthStatus(w http.ResponseWriter, r *http.Request) {
	obj := &healthStatusDTO{
		Status:  ok,
		Modules: make([]*subModule, 0),
	}

	for k, check := range checkMap {
		msg, s := check()
		if s != ok {
			obj.Status = s
			obj.Modules = append(obj.Modules, &subModule{Name: k, Status: s, Message: msg})
		}
	}

	d, _ := json.Marshal(obj)
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Typ", "application/json; charset=UTF-8")
	w.Write(d)
}

func init() {
	inputs.Add("selfstat", func() telegraf.Input {
		return &manager{
			PrometheusPath:   "/metrics",
			HealthStatusPath: "/_api/health",
		}
	})
}
