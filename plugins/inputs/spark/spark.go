package spark

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
)

// api prefix
const (
	sparkAppPath = "api/v1/applications"
	// nano seconds to mile second
	ns2ms = 1000000
)

type Spark struct {
	URLS            []string        `toml:"urls"`
	ResponseTimeout config.Duration `toml:"timeout"`

	JobInclude   bool `toml:"job_include"`
	StageInclude bool `toml:"stage_include"`

	app    []*sparkApp
	client *http.Client
	// globalTags map[string]string

	tls.ClientConfig

	K8SServiceDiscovery *kubernetes.ServiceFilter `toml:"k8s_service_discovery"`
}

type sparkApp struct {
	ID       string             `json:"id"`
	Name     string             `json:"name"`
	driverEx chan executorModel // driver is a executor whose id equal driver
	client   *http.Client
	url      string
}

func (sp *Spark) Description() string {
	return "Gather metric about Spark"
}

func (sp *Spark) SampleConfig() string {
	return `
  ## for kubernetes, the spark_url is the driver's ui url.
  ## "http://<driver_ip>:4040" by default, where <driver_ip> is the
  ## IP address or resolvable spark driver service name.
  spark_url = "${SPARK_URL}"

  ## Set response_timeout (default 10 seconds)
  timeout = "10s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`
}

func (sp *Spark) Init() error {
	tlsCfg, err := sp.ClientConfig.TLSConfig()
	if err != nil {
		return err
	}
	sp.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: time.Duration(sp.ResponseTimeout),
	}
	if sp.K8SServiceDiscovery != nil {
		services, err := kubernetes.ListService(*sp.K8SServiceDiscovery)
		if err != nil {
			return err
		}
		urls := []string{}
		for _, s := range services {
			if s.Protocol == "TCP" {
				urls = append(urls, fmt.Sprintf("http://%s:%d", s.Address, s.Port))
			}
		}
		sp.URLS = urls
	}

	return nil
}

func (sp *Spark) Gather(acc telegraf.Accumulator) error {
	if err := sp.Init(); err != nil {
		return err
	}
	allApps := []*sparkApp{}
	for _, url := range sp.URLS {
		apps, err := sp.getRunningApps(url, sp.client)
		if err != nil {
			acc.AddError(fmt.Errorf("failed to get running app. err: %s", err))
			continue
		}
		allApps = append(allApps, apps...)
	}
	var wg sync.WaitGroup
	wg.Add(len(allApps))
	for _, app := range allApps {
		go func(_app *sparkApp) {
			defer wg.Done()
			sp.gatherFromApp(app, acc)
		}(app)
	}
	wg.Wait()

	return nil
}

func (sp *Spark) gatherFromApp(app *sparkApp, acc telegraf.Accumulator) {
	gtags := make(map[string]string)
	gtags["spark_app_id"] = app.ID
	gtags["spark_app_name"] = app.Name
	now := time.Now()
	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		defer wg.Done()
		if err := app.gatherDriverMetrics(copyMap(gtags), acc, now); err != nil {
			acc.AddError(err)
		}
	}()

	go func() {
		wg.Add(1)
		defer wg.Done()
		if err := app.gatherExecutorMetrics(copyMap(gtags), acc, now); err != nil {
			acc.AddError(err)
		}
	}()

	if sp.JobInclude {
		go func() {
			wg.Add(1)
			defer wg.Done()
			if err := app.gatherJobMetrics(copyMap(gtags), acc, now); err != nil {
				acc.AddError(err)
			}
		}()
	}

	if sp.StageInclude {
		go func() {
			wg.Add(1)
			defer wg.Done()
			if err := app.gatherStageMetrics(copyMap(gtags), acc, now); err != nil {
				acc.AddError(err)
			}
		}()
	}
	wg.Wait()
}

// get spark running application, on dice there is only one application
func (sp *Spark) getRunningApps(sparkURL string, client *http.Client) ([]*sparkApp, error) {
	var apps []*sparkApp
	if err := doReqAndJson(sp.client, fmt.Sprintf("%s/%s?status=running", sparkURL, sparkAppPath), &apps); err != nil {
		return nil, err
	}
	if len(apps) == 0 {
		return nil, fmt.Errorf("no running app. passed")
	}
	for i := range apps {
		apps[i].driverEx = make(chan executorModel)
		apps[i].url = sparkURL
		apps[i].client = client
	}
	return apps, nil
}

func (app *sparkApp) gatherJobMetrics(tags map[string]string, acc telegraf.Accumulator, now time.Time) error {
	url := fmt.Sprintf("%s/%s/%s/jobs", app.url, sparkAppPath, app.ID)
	allItems := make([]*jobModel, 0)
	if err := doReqAndJson(app.client, url, &allItems); err != nil {
		return err
	}
	nItem := len(allItems)
	if nItem == 0 {
		return nil
	}

	for i := 0; i < nItem; i++ {
		job := allItems[i]
		tags["job_id"] = strconv.Itoa(job.JobId)
		tags["job_name"] = job.Name
		tags["status"] = job.Status

		fields := make(map[string]interface{})
		if err := populateMetric(fields, job); err != nil {
			continue
		}
		if job.Status == "SUCCEEDED" {
			fields["running_duration"] = int((job.CompletionTime.UnixNano() - job.SubmissionTime.UnixNano()) / ns2ms)
		} else {
			fields["running_duration"] = -1
		}
		fields["count"] = nItem
		fields["num_stages"] = len(job.StageIds)

		acc.AddFields("spark_job", fields, tags, now)
	}
	return nil
}

func (app *sparkApp) gatherStageMetrics(tags map[string]string, acc telegraf.Accumulator, now time.Time) error {
	url := fmt.Sprintf("%s/%s/%s/stages", app.url, sparkAppPath, app.ID)
	allItems := make([]*stageModel, 0)
	if err := doReqAndJson(app.client, url, &allItems); err != nil {
		return err
	}
	nItem := len(allItems)
	if nItem == 0 {
		return nil
	}

	for i := 0; i < nItem; i++ {
		stage := allItems[i]
		tags["stage_id"] = strconv.Itoa(stage.StageId)
		tags["stage_name"] = stage.Name
		tags["status"] = stage.Status

		fields := make(map[string]interface{})
		if err := populateMetric(fields, stage); err != nil {
			continue
		}
		if stage.Status == "COMPLETE" {
			fields["running_duration"] = int((stage.CompletionTime.UnixNano() - stage.SubmissionTime.UnixNano()) / ns2ms)
		} else {
			fields["running_duration"] = -1
		}
		fields["count"] = nItem

		acc.AddFields("spark_stage", fields, tags, now)
	}
	return nil
}

func (app *sparkApp) gatherExecutorMetrics(tags map[string]string, acc telegraf.Accumulator, now time.Time) error {
	url := fmt.Sprintf("%s/%s/%s/executors", app.url, sparkAppPath, app.ID)
	allItems := make([]*executorModel, 0)
	if err := doReqAndJson(app.client, url, &allItems); err != nil {
		return err
	}
	nItem := len(allItems)
	if nItem == 0 {
		return nil
	}

	for i := 0; i < nItem; i++ {
		ex := allItems[i]
		// transfer driverEx to gatherDriverMetrics goroutine
		if ex.Id == "driver" {
			go func() { app.driverEx <- *ex }()
			continue
		}

		tags["executor_id"] = ex.Id
		tags["executor_host_port"] = ex.HostPort

		fields := make(map[string]interface{})
		if err := populateMetric(fields, ex); err != nil {
			continue
		}
		fields["count"] = nItem

		acc.AddFields("spark_executor", fields, tags, now)
	}
	return nil
}

func (app *sparkApp) gatherDriverMetrics(tags map[string]string, acc telegraf.Accumulator, now time.Time) error {
	fieldsChan := make(chan map[string]interface{})
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)
	// driver info
	go func() {
		defer wg.Done()
		to := time.After(time.Second * 20)
		select {
		case ex := <-app.driverEx:
			exFields := make(map[string]interface{})
			if err := populateMetric(exFields, &ex); err != nil {
				acc.AddError(err)
				return
			}
			fieldsChan <- exFields
		case <-to:
			acc.AddError(fmt.Errorf("timeout of receice driverEx"))
			return
		}
	}()
	// driver jvm info
	go func() {
		defer wg.Done()
		url := fmt.Sprintf("%s/metrics/json", app.url)
		var metricsJson struct {
			Gauges map[string]map[string]interface{} `json:"gauges"`
		}
		if err := doReqAndJson(app.client, url, &metricsJson); err != nil {
			acc.AddError(err)
			return
		}
		var jvm driverJvm
		if err := unmarshalJvm(metricsJson.Gauges, &jvm); err != nil {
			acc.AddError(err)
			return
		}
		jvmFields := make(map[string]interface{})
		if err := populateMetric(jvmFields, &jvm); err != nil {
			acc.AddError(err)
			return
		}
		fieldsChan <- jvmFields
	}()

	go func() {
		wg.Wait()
		close(fieldsChan)
		close(done)
	}()

	go func() {
		fields := make(map[string]interface{})
		for field := range fieldsChan {
			for k, v := range field {
				fields[k] = v
			}
		}
		acc.AddFields("spark_driver", fields, tags, now)
	}()

	<-done
	return nil
}

func doReqAndJson(client *http.Client, url string, receiver interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad http code: %d", resp.StatusCode)
	}
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, &receiver); err != nil {
		return err
	}
	return nil
}

func copyMap(m map[string]string) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}

func init() {
	inputs.Add("spark", func() telegraf.Input {
		return &Spark{
			ResponseTimeout: config.Duration(time.Second * 10),
		}
	})
}
