package prometheus

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const acceptHeader = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3`

type Prometheus struct {
	// An array of urls to scrape metrics from.
	URLs       []string `toml:"urls"`
	MetricName string   `toml:"metric_name"`

	// Bearer Token authorization file path
	BearerToken       string          `toml:"bearer_token"`
	BearerTokenString string          `toml:"bearer_token_string"`
	ResponseTimeout   config.Duration `toml:"response_timeout"`
	tls.ClientConfig
	client *http.Client
	wg     sync.WaitGroup
}

var sampleConfig = `
  ## An array of urls to scrape metrics from.
  urls = ["http://localhost:9100/metrics"]

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

func (p *Prometheus) SampleConfig() string {
	return sampleConfig
}

func (p *Prometheus) Description() string {
	return "Read metrics from one or many prometheus clients"
}

var ErrProtocolError = errors.New("prometheus protocol error")

func (p *Prometheus) AddressToURL(u *url.URL, address string) *url.URL {
	host := address
	if u.Port() != "" {
		host = address + ":" + u.Port()
	}
	reconstructedURL := &url.URL{
		Scheme:     u.Scheme,
		Opaque:     u.Opaque,
		User:       u.User,
		Path:       u.Path,
		RawPath:    u.RawPath,
		ForceQuery: u.ForceQuery,
		RawQuery:   u.RawQuery,
		Fragment:   u.Fragment,
		Host:       host,
	}
	return reconstructedURL
}

type URLAndAddress struct {
	OriginalURL *url.URL
	URL         *url.URL
	Address     string
	MetricName  string
	Tags        map[string]string
}

func (p *Prometheus) GetAllURLs() (map[string]URLAndAddress, error) {
	allURLs := make(map[string]URLAndAddress, 0)
	for _, u := range p.URLs {
		if u == "" {
			continue
		}
		URL, err := url.Parse(u)
		if err != nil {
			log.Printf("prometheus: Could not parse %s, skipping it. Error: %s", u, err.Error())
			continue
		}
		allURLs[URL.String()] = URLAndAddress{URL: URL, OriginalURL: URL, MetricName: p.MetricName}
	}
	return allURLs, nil
}

// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (p *Prometheus) Gather(acc telegraf.Accumulator) error {
	if p.client == nil {
		client, err := p.createHTTPClient()
		if err != nil {
			return err
		}
		p.client = client
	}

	var wg sync.WaitGroup

	allURLs, err := p.GetAllURLs()
	if err != nil {
		return err
	}
	for _, URL := range allURLs {
		wg.Add(1)
		go func(serviceURL URLAndAddress) {
			defer wg.Done()
			acc.AddError(p.gatherURL(serviceURL, acc))
		}(URL)
	}

	wg.Wait()

	return nil
}

func (p *Prometheus) createHTTPClient() (*http.Client, error) {
	tlsCfg, err := p.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   tlsCfg,
			DisableKeepAlives: true,
		},
		Timeout: time.Duration(p.ResponseTimeout),
	}

	return client, nil
}

func (p *Prometheus) gatherURL(u URLAndAddress, acc telegraf.Accumulator) error {
	var req *http.Request
	var err error
	var uClient *http.Client
	if u.URL.Scheme == "unix" {
		path := u.URL.Query().Get("path")
		if path == "" {
			path = "/metrics"
		}
		req, err = http.NewRequest("GET", "http://localhost"+path, nil)

		// ignore error because it's been handled before getting here
		tlsCfg, _ := p.ClientConfig.TLSConfig()
		uClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   tlsCfg,
				DisableKeepAlives: true,
				Dial: func(network, addr string) (net.Conn, error) {
					c, err := net.Dial("unix", u.URL.Path)
					return c, err
				},
			},
			Timeout: time.Duration(p.ResponseTimeout),
		}
	} else {
		if u.URL.Path == "" {
			u.URL.Path = "/metrics"
		}
		req, err = http.NewRequest("GET", u.URL.String(), nil)
	}

	req.Header.Add("Accept", acceptHeader)

	if p.BearerToken != "" {
		token, err := ioutil.ReadFile(p.BearerToken)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+string(token))
	} else if p.BearerTokenString != "" {
		req.Header.Set("Authorization", "Bearer "+p.BearerTokenString)
	}

	var resp *http.Response
	if u.URL.Scheme != "unix" {
		resp, err = p.client.Do(req)
	} else {
		resp, err = uClient.Do(req)
	}
	if err != nil {
		if urlErr, ok := err.(*url.Error); ok {
			if urlErr.Err != nil {
				if netError, ok := urlErr.Err.(*net.OpError); ok {
					if netError.Op == "dial" && strings.Contains(netError.Error(), "connection refused") {
						return nil // 说明 当前机器上不存在sidecar, 直接返回
					}
				}
			}
		}
		return fmt.Errorf("error making HTTP request to %s: %s", u.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", u.URL, resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %s", err)
	}

	metrics, err := Parse(body, resp.Header)
	if err != nil {
		return fmt.Errorf("error reading metrics for %s: %s",
			u.URL, err)
	}

	metricsMap := make(map[uint64]*cachedMetric)

	summaryMetricsMap := make(map[string]*cachedMetric)
	for _, item := range metrics {
		id, tags := getMetricKey(item), item.Tags()
		m, ok := metricsMap[id]
		if !ok {
			if inter, ok := item.GetTag("interface"); ok {
				idx := strings.LastIndex(inter, ":")
				if idx >= 0 {
					parts := strings.Split(inter[idx+1:], "_")
					if len(parts) == 3 {
						tags["version"] = parts[0]
						tags["project_id"] = parts[1]
						tags["workspace"] = strings.ToUpper(parts[2])
						tags["interface"] = inter[0:idx]
					}
				}
			}
			if host, ok := item.GetTag("host"); ok {
				tags["addr"] = host
				delete(tags, "host")
			}
			if wk, ok := tags["workspace"]; ok {
				tags["workspace"] = strings.ToUpper(wk)
			}
			if env, ok := tags["env"]; ok {
				tags["workspace"] = strings.ToUpper(env)
				delete(tags, "env")
			}

			// strip user and password from URL
			// u.OriginalURL.User = nil
			// tags["url"] = u.OriginalURL.String()
			// if u.Address != "" {
			// 	tags["address"] = u.Address
			// }
			for k, v := range u.Tags {
				tags[k] = v
			}
			m = &cachedMetric{
				Fields:    make(map[string]interface{}),
				Tags:      tags,
				Timestamp: item.Time(),
			}
			metricsMap[id] = m
		}
		fields := item.FieldList()
		if len(fields) == 1 {
			m.Fields[item.Name()] = fields[0].Value
		} else {
			for _, value := range fields {
				m.Fields[item.Name()+"_"+value.Key] = value.Value
			}
		}
	}

	for _, m := range metricsMap {
		acc.AddFields(u.MetricName, m.Fields, m.Tags, m.Timestamp)
		if pid, ok := m.Tags["project_id"]; ok {
			if workspace, ok := m.Tags["workspace"]; ok {
				key := pid + "_" + workspace
				sm, ok := summaryMetricsMap[key]
				if !ok {
					sm = &cachedMetric{
						Fields: make(map[string]interface{}),
						Tags: map[string]string{
							"project_id": pid,
							"workspace":  workspace,
						},
						Timestamp: m.Timestamp,
					}
					summaryMetricsMap[key] = sm
				}
				if v, ok := m.Fields["upstream_request_total"]; ok {
					sm.Fields["upstream_request_total"] = toUint64(sm.Fields["upstream_request_total"], 0) + toUint64(v, 0)
				}
				if v, ok := m.Fields["upstream_request_duration_time_total"]; ok {
					sm.Fields["upstream_request_duration_time_total"] = toUint64(sm.Fields["upstream_request_duration_time_total"], 0) + toUint64(v, 0)
				}
			}
		}
	}
	sumMetricName := u.MetricName + "_summary"
	for _, m := range summaryMetricsMap {
		if len(m.Fields) <= 0 {
			continue
		}
		acc.AddFields(sumMetricName, m.Fields, m.Tags, m.Timestamp)
	}
	return nil
}

// toUint64 将interface{}转换为uint64，失败则返回defVal
func toUint64(obj interface{}, defVal uint64) uint64 {
	switch val := obj.(type) {
	case int:
		return uint64(val)
	case int8:
		return uint64(val)
	case int16:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return uint64(val)
	case float32:
		return uint64(val)
	case float64:
		return uint64(val)
	}
	return defVal
}

type cachedMetric struct {
	Fields    map[string]interface{}
	Tags      map[string]string
	Timestamp time.Time
}

func getMetricKey(m telegraf.Metric) uint64 {
	h := fnv.New64a()
	for _, tag := range m.TagList() {
		h.Write([]byte(tag.Key))
		h.Write([]byte("\n"))
		h.Write([]byte(tag.Value))
		h.Write([]byte("\n"))
	}
	return h.Sum64()
}

func init() {
	inputs.Add("termiuns_sidecar", func() telegraf.Input {
		return &Prometheus{
			ResponseTimeout: config.Duration(time.Second * 3),
		}
	})
}
