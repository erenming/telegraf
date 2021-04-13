package spotnotify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

const NotifyMetricName = "spot_notify"

type SpotNotify struct {
	DingDing     []string `toml:"dingding"`
	EventBoxAddr string   `toml:"event_box"`
}

func (d *SpotNotify) Connect() error {
	return nil
}

func (d *SpotNotify) Close() error {
	return nil
}

func (d *SpotNotify) Description() string {
	return ""
}

func (d *SpotNotify) SampleConfig() string {
	return ""
}

func (d *SpotNotify) Write(metrics []telegraf.Metric) error {
	var err error
	eventMap := d.aggregateEvent(metrics)
	for _, e := range eventMap {
		err = d.sendNotify(e)
	}
	return err
}

func (d *SpotNotify) sendNotify(metrics []telegraf.Metric) error {
	if notify, hasVal := d.aggregateNotify(metrics); hasVal {
		body, err := d.formatEventNotify(notify)
		if err != nil {
			log.Printf("E! Format notify message error %s", err.Error())
			return err
		}
		for _, url := range d.DingDing {
			err = d.sendDMsg(url, body)
			if err != nil {
				log.Printf("E! Send notify error %s", err.Error())
				return err
			}
		}
		log.Printf("I! Send notify to %s . message: %s", d.EventBoxAddr, string(body))
	}
	return nil
}

func (d *SpotNotify) sendDMsg(dingURL string, body []byte) error {
	resp, err := http.Post(dingURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	if resp.Body != nil {
		err := resp.Body.Close()
		if err != nil {
			log.Printf("E! Close resp error %s", err.Error())
		}
	}
	return nil
}

func (d *SpotNotify) formatEventNotify(metric telegraf.Metric) ([]byte, error) {
	event := make(map[string]interface{})
	event["msgtype"] = "text"
	content := fmt.Sprintf("事件: %s\n时间: %s\n集群: %s\n%s", metric.Tags()["event"], metric.Time(), metric.Tags()["cluster_name"], metric.Tags()["message"])
	event["text"] = map[string]string{"content": content}
	return json.Marshal(event)
}

func (d *SpotNotify) aggregateEvent(metrics []telegraf.Metric) [][]telegraf.Metric {
	eventMap := make(map[string]map[string]telegraf.Metric)
	for _, m := range metrics {
		if m.Name() == NotifyMetricName {
			if key, hasVal := m.GetTag("notify_key"); hasVal {
				events, hasVal := eventMap[key]
				if !hasVal {
					events = make(map[string]telegraf.Metric)
				}
				events[m.Tags()["component"]] = m
				eventMap[key] = events
			}
		}
	}
	results := make([][]telegraf.Metric, 0)
	for _, events := range eventMap {
		result := make([]telegraf.Metric, 0)
		for _, e := range events {
			result = append(result, e)
		}
		results = append(results, result)
	}
	return results
}

func (d *SpotNotify) aggregateNotify(metrics []telegraf.Metric) (telegraf.Metric, bool) {
	var final telegraf.Metric
	var message string
	for _, m := range metrics {
		final = m
		message = message + m.Tags()["message"]
	}
	if final != nil {
		final.RemoveTag("message")
		final.AddTag("message", message)
		return final, true
	}
	log.Printf("E! aggregateNotify nil")
	return nil, false
}

func init() {
	outputs.Add("spot_notify", func() telegraf.Output {
		return &SpotNotify{}
	})
}
