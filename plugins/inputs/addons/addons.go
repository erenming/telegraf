package addons

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/inputs"
	gdocker "github.com/influxdata/telegraf/plugins/inputs/global/docker"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/toml"
	"github.com/influxdata/toml/ast"
)

var dockerEventFilterArgs = []filters.KeyValuePair{
	{Key: "type", Value: "container"},
	{Key: "event", Value: "die"},
	{Key: "event", Value: "destroy"},
	{Key: "event", Value: "kill"},
	{Key: "event", Value: "oom"},
	{Key: "event", Value: "stop"},
	{Key: "event", Value: "pause"},
	{Key: "event", Value: "start"},
	{Key: "event", Value: "unpause"},
}

var startInputEvents = map[string]struct{}{
	"start":   struct{}{},
	"unpause": struct{}{},
}
var stopInputEvents = map[string]struct{}{
	"die":     struct{}{},
	"destroy": struct{}{},
	"kill":    struct{}{},
	"oom":     struct{}{},
	"stop":    struct{}{},
	"pause":   struct{}{},
}

type Addons struct {
	InputKeyName      string              `toml:"input_key_name"`
	TemplateKeyName   string              `toml:"template_key_name"`
	Templates         map[string]Template `toml:"config_template"`
	GatherTimeout     config.Duration     `toml:"gather_timeout"`
	FullCheckInterval config.Duration     `toml:"full_check_interval"`

	TagEnv       []string `toml:"tag_env"`
	tagEnvFilter filter.Filter

	inputs     map[string]*inputInfo
	inputsLock sync.RWMutex

	parser   parsers.Parser
	parserFn parsers.ParserFunc

	closeCh chan struct{}

	// // 兼容老的k8s的addon没有SELF
	// ClusterType     string           `toml:"cluster_type"`
	// K8sURLs         []string         `toml:"k8s_urls"`
	// K8sBearerToken  string           `toml:"k8s_bearer_token"`
	// K8sClientConfig tls.ClientConfig `toml:"k8s_client_config"`
	// K8sTimeout      time.Duration    `toml:"k8s_timeout"`
	// k8sClients      []*k8s.Client
	// k8sClientsIdx   int
}

type Template struct {
	Config   string
	cfgTmp   *template.Template
	EnvMatch map[string]string `toml:"env_match"`
}

type inputInfo struct {
	envs   map[string]string
	tags   map[string]string
	name   string
	inputs []telegraf.Input
}

var sampleConfig = `
`

func (a *Addons) SampleConfig() string {
	return sampleConfig
}

func (a *Addons) Description() string {
	return "gather addons metrics"
}

func (a *Addons) Gather(acc telegraf.Accumulator) error {
	errsCh := make(chan error)
	count := 0
	a.inputsLock.RLock()
	for key, info := range a.inputs {
		addAcc := accumulatorWithTags{acc, info.tags}
		for _, input := range info.inputs {
			count++
			go func(input telegraf.Input, acc telegraf.Accumulator) {
				err := input.Gather(acc)
				if err != nil {
					errsCh <- fmt.Errorf("fail to gather addon %s, %s", key, err)
				} else {
					errsCh <- nil
				}
			}(input, addAcc)
		}
		log.Printf("Gatter addon %s, %s, %s ", info.name, key, info.tags["addon_id"])
	}
	a.inputsLock.RUnlock()
	var errs Errors
	tCh := time.After(time.Duration(a.GatherTimeout))
	for i := 0; i < count; i++ {
		select {
		case err := <-errsCh:
			if err != nil {
				errs = append(errs, err)
			}
		case <-tCh:
			errs = append(errs, fmt.Errorf("gather addons timeout"))
			break
		}
	}
	return errs.MaybeUnwrap()
}

func (a *Addons) Start(acc telegraf.Accumulator) error {
	err := a.init()
	if err != nil {
		return fmt.Errorf("fail to init: %s", err)
	}
	go func() {
		now := time.Now()
		err = a.loadAddons(acc)
		if err != nil {
			log.Printf("fail to load addons: %s", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		options := types.EventsOptions{
			Filters: filters.NewArgs(dockerEventFilterArgs...),
			Since:   now.Format(time.RFC3339),
		}
		client, _ := gdocker.GetClient()
		events, errs := client.Events(ctx, options)
		timerCh := time.Tick(time.Duration(a.FullCheckInterval))
		for {
			select {
			case evt, ok := <-events:
				if !ok {
					return
				}
				if _, ok := startInputEvents[evt.Action]; ok {
					_, err := a.checkAndAddInput(evt.ID, acc)
					if err != nil {
						log.Printf("E! [addon] checkAndAddInput(%s) : %v", evt.ID, err)
					}
				} else if _, ok := stopInputEvents[evt.Action]; ok {
					err := a.checkAndStop(evt.ID)
					if err != nil {
						log.Printf("E! [addon] checkAndStop(%s) : %v", evt.ID, err)
					}
				}
			case <-timerCh:
				err = a.loadAddons(acc)
				if err != nil {
					log.Printf("E! [addon] fail to check addons: %s", err)
				}
			case <-a.closeCh:
				return
			case err := <-errs:
				if err == io.EOF {
					return
				}
				log.Printf("E! [addon] error event: %v", err)
			}
		}
	}()
	return nil
}

func (a *Addons) Stop() {
	close(a.closeCh)
	a.inputsLock.Lock()
	defer a.inputsLock.Unlock()
	log.Printf("Stop, delete input")
	for id, info := range a.inputs {
		a.deleteInput(id, info)
	}
}

func (a *Addons) loadAddons(acc telegraf.Accumulator) error {
	client, timeout := gdocker.GetClient()
	for client == nil {
		// 可能 gdocker 还没初始化好
		time.Sleep(1 * time.Second)
		client, timeout = gdocker.GetClient()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	containers, err := client.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return fmt.Errorf("get docker container list: %s", err)
	}
	all := make(map[string]struct{})
	for _, container := range containers {
		key, err := a.checkAndAddInput(container.ID, acc)
		if err != nil {
			log.Printf("E! [addon] loadAddons.checkAndAddInput(%s) : %v", container.ID, err)
			continue
		}
		if key != "" {
			all[key] = struct{}{}
		}
	}
	a.inputsLock.Lock()
	for key, info := range a.inputs {
		if _, ok := all[key]; !ok {
			log.Printf("load addons, delete input, key:%s, inputs:%+v, all:%+v", key, a.inputs, all)
			a.deleteInput(key, info)
		}
	}
	a.inputsLock.Unlock()
	return nil
}

func (a *Addons) checkAndAddInput(cid string, acc telegraf.Accumulator) (string, error) {
	envs, typ, aid, key, err := a.getContainerEnv(cid)
	if err != nil {
		return "", err
	}
	if envs == nil {
		return "", nil
	}
	// 以前一个addon一个telegraf 时，telegraf里也会有ADDON_TYPE，避免匹配这些容器
	if envs["DICE_COMPONENT"] == "spot/telegraf" {
		return "", nil
	}
	a.inputsLock.Lock()
	defer a.inputsLock.Unlock()
	info, ok := a.inputs[key]
	if ok {
		if reflect.DeepEqual(info.envs, envs) {
			return key, nil
		} else {
			log.Printf("checkAndAddInput, delete input, info:%+v, env:%+v", info.envs, envs)
			a.deleteInput(key, info)
		}
	}
	if t, ok := a.Templates[typ]; ok {
		for k, v := range t.EnvMatch {
			if v != "*" {
				if envs[k] != v {
					// not match
					return "", nil
				}
			} else {
				if _, ok := envs[k]; !ok {
					// not match
					return "", nil
				}
			}
		}
		envTags := make(map[string]string)
		for k, v := range envs {
			if a.tagEnvFilter.Match(k) {
				envTags[strings.ToLower(k)] = v
			}
		}
		acc = accumulatorWithTags{acc, envTags}
		info := &inputInfo{
			envs: envs,
			tags: envTags,
			name: typ,
		}
		a.inputs[key] = info
		buf := &bytes.Buffer{}
		err = t.cfgTmp.Execute(buf, envs)
		if err != nil {
			log.Printf("render template failed, error:%v", err)
			return "", err
		}
		tbl, err := toml.Parse(buf.Bytes())
		if err != nil {
			log.Printf("parse envs failed, envs:%+v", envs)
			return "", err
		}
		for inputName, val := range tbl.Fields {
			switch subTable := val.(type) {
			case *ast.Table:
				if err = a.addInput(inputName, info, subTable, acc); err != nil {
					return "", err
				}
			case []*ast.Table:
				for _, t := range subTable {
					if err = a.addInput(inputName, info, t, acc); err != nil {
						return "", err
					}
				}
			default:
				return "", fmt.Errorf("%s: invalid configuration", inputName)
			}
		}
		log.Printf("I! [addon] add input %s, key=%s, aid=%s, cid=%s ok", typ, key, aid, cid)
		return key, nil
	}
	return "", nil
}

func (a *Addons) addInput(name string, info *inputInfo, tbl *ast.Table, acc telegraf.Accumulator) error {
	creator, ok := inputs.Inputs[name]
	if !ok {
		return fmt.Errorf("input plugin %s not exist", name)
	}
	input := creator()
	switch t := input.(type) {
	case parsers.ParserInput:
		t.SetParser(a.parser)
	}
	switch t := input.(type) {
	case parsers.ParserFuncInput:
		t.SetParserFunc(a.parserFn)
	}
	if err := toml.UnmarshalTable(tbl, input); err != nil {
		return fmt.Errorf("parse input %s error: %s", name, err)
	}
	if si, ok := input.(telegraf.ServiceInput); ok {
		err := si.Start(acc)
		if err != nil {
			return fmt.Errorf("Service for input %s failed to start: %v", name, err)
		}
	}
	info.inputs = append(info.inputs, input)
	return nil
}

func (a *Addons) checkAndStop(cid string) error {
	envs, _, _, key, err := a.getContainerEnv(cid)
	if err != nil {
		return err
	}
	if envs != nil {
		return nil
	}
	a.inputsLock.Lock()
	if info, ok := a.inputs[key]; ok {
		log.Printf("checkAndStop, key:%s, input:%+v", key, a.inputs)
		a.deleteInput(key, info)
	}
	a.inputsLock.Unlock()
	return nil
}

func (a *Addons) deleteInput(key string, info *inputInfo) {
	for _, input := range info.inputs {
		if si, ok := input.(telegraf.ServiceInput); ok {
			si.Stop()
		}
	}
	delete(a.inputs, key)
	log.Printf("I! [addon] delete input %s, key=%s ok", info.name, key)
}

func (a *Addons) getContainerEnv(cid string) (map[string]string, string, string, string, error) {
	client, timeout := gdocker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	inspect, err := client.ContainerInspect(ctx, cid)
	if err != nil {
		return nil, "", "", "", err
	}
	envs := make(map[string]string)
	for _, env := range inspect.Config.Env {
		kv := strings.SplitN(env, "=", 2)
		key := kv[0]
		var val string
		if len(kv) > 1 {
			val = kv[1]
		}
		envs[key] = val
	}
	podName := inspect.Config.Labels["io.kubernetes.pod.name"]
	idx := strings.LastIndex(podName, "-")
	if idx > 0 {
		_, err := strconv.Atoi(podName[idx+1:])
		if err == nil {
			prefix := "N" + podName[idx+1:] + "_"
			for k, v := range envs {
				if strings.HasPrefix(k, prefix) {
					envs[k[len(prefix):]] = v
				}
			}
			if ip, ok := envs["SELF_HOST"]; !ok {
				if id, ok := envs["ADDON_ID"]; ok {
					if typ, ok := envs["ADDON_TYPE"]; ok {
						if _, ok := a.Templates[typ]; ok {
							gtype := typ
							if typ == "terminus-elasticsearch" {
								gtype = "elasticsearch"
								if port, ok := envs["TERMINUS_ELASTICSEARCH_SERVICE_PORT"]; ok {
									envs["SELF_PORT"] = port
								} else {
									envs["SELF_PORT"] = "9200"
								}
								if _, ok := envs["ES_PASSWORD"]; !ok {
									envs["ES_PASSWORD"] = ""
								}

							} else if typ == "mysql" {
								envs["SELF_PORT"] = "3306"
							} else if typ == "redis" {
								if port, ok := envs["REDIS_PORT"]; ok {
									envs["SELF_PORT"] = port
								} else {
									envs["SELF_PORT"] = "6379"
								}
							}
							url := fmt.Sprintf("%s.%s.group-addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
							if _, err := net.LookupHost(url); err == nil {
								envs["SELF_HOST"] = url
							} else {
								url = fmt.Sprintf("%s.%s.addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
								if _, err := net.LookupHost(url); err == nil {
									envs["SELF_HOST"] = url
								}
							}
							// ip, err := a.getPodIP(podName, id, gtype)
							// if err == nil {
							// 	envs["SELF_HOST"] = ip
							// }
						}
					}
				}
			} else {
				isIP, _ := regexp.MatchString(`^(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})(\.(2(5[0-5]{1}|[0-4]\d{1})|[0-1]?\d{1,2})){3}$`, ip)
				if isIP {
					if id, ok := envs["ADDON_ID"]; ok {
						if typ, ok := envs["ADDON_TYPE"]; ok {
							if _, ok := a.Templates[typ]; ok {
								gtype := typ
								if typ == "terminus-elasticsearch" {
									gtype = "elasticsearch"
									if _, ok := envs["ES_PASSWORD"]; !ok {
										envs["ES_PASSWORD"] = ""
									}
								}
								url := fmt.Sprintf("%s.%s.group-addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
								if ns, err := net.LookupHost(url); err == nil {
									if a.containsString(ip, ns) {
										envs["SELF_HOST"] = url
									}
								} else {
									url = fmt.Sprintf("%s.%s.addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
									if ns, err := net.LookupHost(url); err == nil {
										if a.containsString(ip, ns) {
											envs["SELF_HOST"] = url
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	key, ok := envs[a.InputKeyName]
	if !ok {
		return nil, "", "", "", nil
	}
	typ, ok := envs[a.TemplateKeyName]
	if !ok {
		return nil, "", "", "", nil
	}
	if typ == "redis" {
		// 暂时不监控 sentinel、避免redis和sentinel在同一台节点上时，addon_id一样导致冲突
		if envs["REDIS_ROLE"] == "sentinel" {
			return nil, "", "", "", nil
		}
		if strings.Contains(envs["SELF_HOST"], "sentinel") {
			return nil, "", "", "", nil
		}
	}
	return envs, typ, envs["ADDON_ID"], fmt.Sprintf("%s-%s", typ, key), err
}

func (a *Addons) containsString(val string, list []string) bool {
	for _, item := range list {
		if val == item {
			return true
		}
	}
	return false
}

func (a *Addons) SetParser(parser parsers.Parser) {
	a.parser = parser
}

func (a *Addons) SetParserFunc(fn parsers.ParserFunc) {
	a.parserFn = fn
}

func (a *Addons) init() error {
	for k, v := range a.Templates {
		tmpl, err := template.New(k).Parse(v.Config)
		if err != nil {
			return err
		}
		v.cfgTmp = tmpl
		a.Templates[k] = v
	}
	var err error
	a.tagEnvFilter, err = filter.Compile(a.TagEnv)
	if err != nil {
		return fmt.Errorf("fail to compile tag env filter: %s", err)
	}

	return nil
}

// func (a *Addons) initK8sClients() error {
// 	var clients []*k8s.Client
// 	for _, k8sURL := range a.K8sURLs {
// 		if k8sURL == "" {
// 			continue
// 		}
// 		c, err := k8s.NewClient(&k8s.Config{
// 			Clusters: []k8s.NamedCluster{{Name: "cluster", Cluster: k8s.Cluster{
// 				Server:                k8sURL,
// 				InsecureSkipTLSVerify: a.K8sClientConfig.InsecureSkipVerify,
// 				CertificateAuthority:  a.K8sClientConfig.TLSCA,
// 			}}},
// 			Contexts: []k8s.NamedContext{{Name: "context", Context: k8s.Context{
// 				Cluster:   "cluster",
// 				AuthInfo:  "auth",
// 				Namespace: "",
// 			}}},
// 			AuthInfos: []k8s.NamedAuthInfo{{Name: "auth", AuthInfo: k8s.AuthInfo{
// 				Token:             a.K8sBearerToken,
// 				ClientCertificate: a.K8sClientConfig.TLSCert,
// 				ClientKey:         a.K8sClientConfig.TLSKey,
// 			}}},
// 		})
// 		if err != nil {
// 			return err
// 		}
// 		clients = append(clients, c)
// 	}
// 	rand.Seed(time.Now().UnixNano())
// 	a.k8sClientsIdx = rand.Int() % len(clients)
// 	a.k8sClients = clients
// 	return nil
// }

// func (a *Addons) getPodIP(name, addonID, addonType string) (string, error) {
// 	pod := new(v1.Pod)
// 	ctx, cancel := context.WithTimeout(context.Background(), a.K8sTimeout)
// 	defer cancel()
// 	var err error
// 	for i := len(a.k8sClients); i > 0; i-- {
// 		a.k8sClientsIdx = (a.k8sClientsIdx + 1) % len(a.k8sClients)
// 		client := a.k8sClients[a.k8sClientsIdx]
// 		group := fmt.Sprintf("group-addon-%s--%s", addonType, addonID)
// 		err = client.Get(ctx, group, name, pod)
// 		if err != nil {
// 			if e, ok := err.(*k8s.APIError); ok && e.Code == 404 {
// 				group = fmt.Sprintf("addon-%s--%s", addonType, addonID)
// 				err = client.Get(ctx, group, name, pod)
// 				if err != nil {
// 					log.Printf("E! fail to get pod %s,%s from %s error: %v", group, name, client.Endpoint, err)
// 					continue
// 				}
// 			} else {
// 				log.Printf("E! fail to get pod %s,%s from %s error: %v", group, name, client.Endpoint, err)
// 				continue
// 			}
// 		}
// 		log.Printf("I! addon %s got pod ip %s  ok", addonID, pod.Status.GetPodIP())
// 		return pod.Status.GetPodIP(), nil
// 	}
// 	return "", err
// }

func init() {
	inputs.Add("addons", func() telegraf.Input {
		return &Addons{
			InputKeyName:      "ADDON_ID",
			TemplateKeyName:   "ADDON_TYPE",
			GatherTimeout:     config.Duration(1 * time.Minute),
			FullCheckInterval: config.Duration(5 * time.Minute),
			closeCh:           make(chan struct{}),
			inputs:            make(map[string]*inputInfo),
		}
	})
}

type Errors []error

func (errs Errors) Error() string {
	if len(errs) == 0 {
		return ""
	} else if len(errs) == 1 {
		return errs[0].Error()
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "%d error(s) occurred:", len(errs))
	for _, err := range errs {
		fmt.Fprintf(buf, "\n* %s", err)
	}
	return buf.String()
}

func (errs Errors) MaybeUnwrap() error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return errs
	}
}

type accumulatorWithTags struct {
	telegraf.Accumulator
	tags map[string]string
}

func (acc accumulatorWithTags) AddFields(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	for k, v := range acc.tags {
		tags[k] = v
	}
	acc.Accumulator.AddFields(measurement, fields, tags, t...)
}

func (acc accumulatorWithTags) AddGauge(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	for k, v := range acc.tags {
		tags[k] = v
	}
	acc.Accumulator.AddGauge(measurement, fields, tags, t...)
}

func (acc accumulatorWithTags) AddCounter(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	for k, v := range acc.tags {
		tags[k] = v
	}
	acc.Accumulator.AddCounter(measurement, fields, tags, t...)
}

func (acc accumulatorWithTags) AddSummary(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	for k, v := range acc.tags {
		tags[k] = v
	}
	acc.Accumulator.AddSummary(measurement, fields, tags, t...)
}

func (acc accumulatorWithTags) AddHistogram(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	for k, v := range acc.tags {
		tags[k] = v
	}
	acc.Accumulator.AddHistogram(measurement, fields, tags, t...)
}

func (acc accumulatorWithTags) AddMetric(metric telegraf.Metric) {
	for k, v := range acc.tags {
		metric.AddTag(k, v)
	}
	acc.Accumulator.AddMetric(metric)
}
