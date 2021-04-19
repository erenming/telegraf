package dcos_app_status

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type DCOSAppStatus struct {
	ClusterUrl string   `toml:"cluster_url"`
	Apps       []string `toml:"apps"`
	Groups     []string `toml:"groups"`

	tls.ClientConfig
	client  *http.Client
	Timeout config.Duration
}

func New() *DCOSAppStatus {
	d := DCOSAppStatus{
		Apps:    []string{},
		Groups:  []string{},
		Timeout: config.Duration(time.Second * 15),
	}
	tlsCfg, _ := d.ClientConfig.TLSConfig()
	d.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: time.Duration(d.Timeout),
	}
	return &d
}

func (d *DCOSAppStatus) SampleConfig() string {
	return ``
}

// Description returns a one-sentence description on the Input
func (d *DCOSAppStatus) Description() string {
	return "Read app metrics from marathon api"
}

func (d *DCOSAppStatus) Gather(acc telegraf.Accumulator) error {

	for _, app := range d.Apps {
		go func(d *DCOSAppStatus, acc telegraf.Accumulator, app string) {
			url := formatAppUrl(d, app)
			data, err := d.requestMarathon(url)
			if err != nil {
				log.Printf("E! Request dc/os api error. %s", err.Error())
				return
			}
			status, err := d.getStatusFromApp(data)
			if err != nil {
				log.Printf("E! Convert app status error. %s", err.Error())
				return
			}
			if err := d.gatherStatus(acc, status); err != nil {
				log.Printf("E! Gather app status error. %s", err.Error())
			}
		}(d, acc, app)
	}

	for _, group := range d.Groups {
		go func(d *DCOSAppStatus, acc telegraf.Accumulator, group string) {
			url := formatGroupUrl(d, group)
			data, err := d.requestMarathon(url)
			if err != nil {
				log.Printf("E! Request dc/os api error. %s", err.Error())
				return
			}
			status, err := d.getStatusFromGroup(data)
			if err != nil {
				log.Printf("E! Convert app status error. %s", err.Error())
				return
			}
			if err := d.gatherStatus(acc, status); err != nil {
				log.Printf("E! Gather app status error. %s", err.Error())
			}
		}(d, acc, group)
	}

	return nil
}

func (d *DCOSAppStatus) requestMarathon(url string) ([]byte, error) {
	resp, err := d.client.Get(url)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (d *DCOSAppStatus) getStatusFromApp(data []byte) ([]*AppStatus, error) {
	var app DCOSApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, err
	}
	return []*AppStatus{app.App}, nil
}

func (d *DCOSAppStatus) getStatusFromGroup(data []byte) ([]*AppStatus, error) {
	var group DCOSGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, err
	}
	return group.Group, nil
}

func (d *DCOSAppStatus) gatherStatus(acc telegraf.Accumulator, status []*AppStatus) error {
	for _, s := range status {
		component := s.Id
		names := strings.Split(s.Id, "/")
		if len(names) > 0 {
			component = names[len(names)-1]
		}

		tags := map[string]string{"marathon_app_id": s.Id, "component_name": component}
		fields := map[string]interface{}{"instances": s.Instances, "tasks_healthy": s.TasksHealthy, "tasks_running": s.TasksRunning, "tasks_staged": s.TasksStaged, "tasks_unhealthy": s.TasksUnhealthy}
		m, err := metric.New("dcos_app_status", tags, fields, time.Now(), telegraf.Untyped)
		if err == nil {
			acc.AddMetric(m)
		} else {
			acc.AddError(err)
		}
	}
	return nil
}

func formatAppUrl(d *DCOSAppStatus, app string) string {
	return d.ClusterUrl + "/service/marathon/v2/apps/" + app
}

func formatGroupUrl(d *DCOSAppStatus, group string) string {
	return d.ClusterUrl + "/service/marathon/v2/apps/" + group + "/*"
}

func init() {
	inputs.Add("dcos_app_status", func() telegraf.Input {
		return New()
	})
}
