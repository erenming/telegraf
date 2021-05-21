package dice_health

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	tmetric "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const diceComponentLabel = "dice/component"

const (
	healthUrlTag     = "health_url"
	diceComponentTag = "dice_component"
	podNameTag       = "dice_pod_name"
	messageTag       = "message"
)

const metricName = "dice_health"

const (
	statusUnavailable = "unavailable"
	status404         = "unknown"
)

type healthResponseBody struct {
	Name    string            `json:"name"`
	Status  string            `json:"status"`
	Modules []checkModule     `json:"modules"`
	Tags    map[string]string `json:"tags"`
}

type checkModule struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Health struct {
	ServiceCheck struct {
		Timeout config.Duration `toml:"timeout"`
	} `toml:"service_check"`
	Exclude                 []string `toml:"exclude"`
	ContentEncoding         string   `toml:"content_encoding"`
	KubernetesLabelSelector string   `toml:"kubernetes_label_selector"`

	componentMap map[string]*component
	client       *http.Client
}

func (h *Health) SampleConfig() string {
	return ""
}

func (h *Health) Description() string {
	return ""
}

func (h *Health) Gather(acc telegraf.Accumulator) error {
	if cm, err := h.getComponentMap(); err != nil {
		return err
	} else {
		h.componentMap = cm
	}

	if h.client == nil {
		h.client = &http.Client{
			Timeout: time.Duration(h.ServiceCheck.Timeout),
		}
	}
	var wg sync.WaitGroup
	for _, c := range h.componentMap {
		wg.Add(1)
		go func(url string, com *component) {
			defer wg.Done()
			if err := h.gatherURL(acc, url, com); err != nil {
				acc.AddError(fmt.Errorf("[url=%s]: %s", url, err))
			}
		}(c.serviceURL, c)
	}
	wg.Wait()
	return nil
}

func (h *Health) gatherURL(
	acc telegraf.Accumulator,
	url string,
	com *component,
) error {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := h.client.Do(request)
	if err != nil {
		acc.AddMetric(constructMetric(statusUnavailable, url, com))
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		acc.AddMetric(constructMetric(status404, url, com))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received status code %d (%s), expected %d (%s)",
			resp.StatusCode,
			http.StatusText(resp.StatusCode),
			http.StatusOK,
			http.StatusText(http.StatusOK))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	m, err := convertToMetric(b, url, com)
	if err != nil {
		return err
	}
	acc.AddMetric(m)
	return nil
}

func constructMetric(status string, url string, com *component) telegraf.Metric {
	tags := map[string]string{
		healthUrlTag:     url,
		diceComponentTag: com.name,
		podNameTag:       com.podName,
		messageTag:       fmt.Sprintf("self_check: dice component %s is %s", com.name, status),
	}
	fields := map[string]interface{}{
		"status": status,
	}
	m, _ := tmetric.New(metricName, tags, fields, time.Now())
	return m
}

func convertToMetric(buf []byte, url string, com *component) (telegraf.Metric, error) {
	resp := healthResponseBody{}
	err := json.Unmarshal(buf, &resp)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]interface{})
	fields["status"] = resp.Status
	fields["modules"] = resp.Modules

	tags := make(map[string]string)
	if resp.Tags != nil {
		tags = resp.Tags
	}
	tags[healthUrlTag] = url
	tags[diceComponentTag] = com.name
	tags[podNameTag] = com.podName
	tags[messageTag] = buildMessage(resp.Modules)
	metric, err := tmetric.New(metricName, tags, fields, time.Now())
	if err != nil {
		return nil, err
	}
	return metric, nil
}

func buildMessage(modules []checkModule) string {
	res := make([]string, len(modules))
	for idx, m := range modules {
		res[idx] = m.Message
	}
	return strings.Join(res, "; ")
}

type component struct {
	name       string
	serviceURL string
	podName    string
	// podMap     map[string]string // <name,ip:port>
}

func existed(s string, items []string) bool {
	for _, item := range items {
		if s == item {
			return true
		}
	}
	return false
}

func (h Health) getComponentMap() (map[string]*component, error) {
	cli, to := kubernetes.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()

	ls, err := labels.Parse(h.KubernetesLabelSelector)
	if err != nil {
		return nil, err
	}
	list, err := cli.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
		LabelSelector: ls.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods failed, err: %s", err)
	}
	// dice component usually have more than two component.
	res := make(map[string]*component, len(list.Items)/2)
	for _, p := range list.Items {
		comName, ok := p.ObjectMeta.Labels[diceComponentLabel]
		if !ok {
			continue
		}
		if existed(comName, h.Exclude) {
			continue
		}
		if _, ok = res[comName]; ok {
			continue
		}
		if len(p.Spec.Containers) == 0 {
			continue
		}

		// all check pass
		firstC := p.Spec.Containers[0]
		c := &component{
			name:    comName,
			podName: p.ObjectMeta.Name,
		}


		// with first port
		if c.serviceURL == "" {
			if len(firstC.Ports) == 0 {
				continue
			}
			c.serviceURL = strings.Join([]string{p.Status.PodIP, strconv.Itoa(int(firstC.Ports[0].ContainerPort))}, ":")
		}
		c.serviceURL = normalizeURL(c.serviceURL)
		res[comName] = c
	}
	return res, nil
}

func normalizeURL(url string) string {
	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}
	if !strings.HasSuffix(url, "/_api/health") {
		url += "/_api/health"
	}
	return url
}

func init() {

	inputs.Add("dice_health", func() telegraf.Input {
		return &Health{}
	})
}
