package metaserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/docker"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

const (
	// 15min 全量同步一把 running containers
	measurementAllRunningContainers = "metaserver_all_containers"
	measurementContainer            = "metaserver_container"
	measurementHost                 = "metaserver_host"
)

const (
	dockerEventActionCreate = "create"
	dockerEventActionStart  = "start"
	dockerEventActionKill   = "kill"
	dockerEventActionOOM    = "oom"
	dockerEventActionDie    = "die"
)

const (
	mesosTaskIDKey = "MESOS_TASK_ID"
)

var initialFilterArgs = []filters.KeyValuePair{
	{Key: "type", Value: "container"},
	{Key: "event", Value: dockerEventActionStart},
	{Key: "event", Value: dockerEventActionKill},
	{Key: "event", Value: dockerEventActionOOM},
	{Key: "event", Value: dockerEventActionDie},
}

type Metaserver struct {
	DockerNetworks []string `toml:"docker_networks"`
	FieldKeys      []string `toml:"field_keys"`
	TagKeys        []string `toml:"tag_keys"`
	IPKey          string   `toml:"ip_key"`
	EtcPath        string   `toml:"etc_path"`
	ProcPath       string   `toml:"proc_path"`

	init                     bool
	watcherStarted           bool          // docker events watcher
	pushFullContainerStarted bool          // 周期性全量更新 containers
	pushHostsStarted         bool          // 周期性推送宿主机信息
	pushPeriod               time.Duration // 更新周期时间

	etcPath         string
	procPath        string
	fieldKeysFilter filter.Filter
	tagKeysFilter   filter.Filter
}

// Description metaserver description
func (t *Metaserver) Description() string {
	return "Read metaserver of machine and docker"
}

// SampleConfig metaserver sample config
func (t *Metaserver) SampleConfig() string {
	return `
	## Docker Endpoint
	## Get from env:DOCKER_ENDPOINT, if empty, get from config file:docker_endpoint
	## DOCKER_ENDPOINT=unix:///var/run/docker.sock
	## Create docker client using environment variables when docker endpoint is ENV 
	docker_endpoint = "unix:///var/run/docker.sock"

	## Timeout for docker client commands
	## Get from config file:docker_timeout
	docker_timeout = "5s"

	## Docker Optional TLS Config
	# tls_ca = "/etc/telegraf/ca.pem"
	# tls_cert = "/etc/telegraf/cert.pem"
	# tls_key = "/etc/telegraf/key.pem"
	## Use TLS but skip chain & host verification
	# insecure_skip_verify = false

	## Docker Key
	## Get docker container environment or label keys
	## Get from env:DOCKER_KEYS, if empty, get from config file:docker_keys
	## DOCKER_KEY=HOST,TERMINUS_KEY,DICE_*
	docker_keys = ["DICE_*"]

	## Network Keys Of Docker Container' NetworkSetting
	## Get ipAddr by order, pick the first matched
	## Get from env:DOCKER_NETWORKS, if empty, get from config file:docker_networks
	## DOCKER_NETWORKS=dcos,host,bridge
	docker_networks = ["dcos", "host", "bridge"]

	## Cluster File Path
	## Get Cluster from this file, this path is absolute path. This file must have Cluster=XXX
	## Get from env:CLUSTER_PATH, if empty, get from config file:cluster_path
	## CLUSTER_PATH=/etc/cluster-node
	cluster_path=/etc/cluster-node

	## Labels File Path
	## Get Labels from this file, this path is absolute path
	## Get from env:LABELS_PATH, if empty, get from config file:labels_path
	## LABELS_PATH=/var/lib/dcos/mesos-slave-common
	labels_path=/var/lib/dcos/mesos-slave-common

	## Container Group Key
	## Container group by this key, this group container will do something like alert and so on
	## Get from env:GROUP_KEY, if empty, get from config file:group_key
	## GROUP_KEY=TERMINUS_GROUP
	group_key=TERMINUS_GROUP
	`
}

func needGather() bool {
	return os.Getenv("XXX_ENABLE_PLUGIN_METASERVER") == "true"
}

// Gather metaserver gather func
func (t *Metaserver) Gather(acc telegraf.Accumulator) error {
	if !needGather() {
		return nil
	}

	var err error
	log.Println("I! start to metaserver gather.")

	if !t.init {
		if err = t.initServer(); err != nil {
			return err
		}

		t.init = true
	}

	// 15min 采集一次全量的 containers
	if !t.pushFullContainerStarted {
		go t.loopFullPushRunningContainers(acc)
	}

	// 启动 docker eventer
	if !t.watcherStarted {
		go t.gatherContainerByDockerEvents(acc)
	}

	// 启动 host pusher
	if !t.pushHostsStarted {
		go t.loopGatherHost(acc)
	}

	return nil
}

// TODO: 数据源转从 cluster 表中获取，这个数据可以作为补充
func (t *Metaserver) loopGatherHost(acc telegraf.Accumulator) {
	t.pushHostsStarted = true
	defer func() {
		t.pushHostsStarted = false
	}()

	interval := time.Minute
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		if err := t.gatherHost(acc); err != nil {
			log.Printf("failed to gather host info: %v", err)
		}

		select {
		case <-timer.C:
			if interval < t.pushPeriod {
				interval *= 2
				if interval > t.pushPeriod {
					interval = t.pushPeriod
				}
			}
		}
		timer.Reset(interval)
	}
}

func (t *Metaserver) loopFullPushRunningContainers(acc telegraf.Accumulator) {
	t.pushFullContainerStarted = true
	defer func() {
		t.pushFullContainerStarted = false
	}()

	ticker := time.NewTicker(t.pushPeriod)
	defer ticker.Stop()

	for {
		if err := t.gatherAllRunningContainers(acc); err != nil {
			log.Printf("failed to gather all running containers: %v", err)
		}

		select {
		case <-ticker.C:
			continue
		}
	}
}

func (t *Metaserver) initServer() error {
	dockerNetworks := os.Getenv("DOCKER_NETWORKS")
	if dockerNetworks != "" {
		t.DockerNetworks = strings.Split(dockerNetworks, ",")
	}
	fieldKeys := os.Getenv("FIELD_KEYS")
	if fieldKeys != "" {
		t.FieldKeys = strings.Split(fieldKeys, ",")
	}
	tagKeys := os.Getenv("TAG_KEYS")
	if tagKeys != "" {
		t.TagKeys = strings.Split(tagKeys, ",")
	}
	ipKey := os.Getenv("IP_KEY")
	if ipKey != "" {
		t.IPKey = ipKey
	}

	etcPath := os.Getenv("HOST_ETC")
	if etcPath != "" {
		t.etcPath = etcPath
	}
	procPath := os.Getenv("HOST_PROC")
	if procPath != "" {
		t.procPath = procPath
	}

	var err error
	t.fieldKeysFilter, err = filter.Compile(t.FieldKeys)
	if err != nil {
		return errors.Errorf("fail to compile field keys filter: %s", err)
	}
	t.tagKeysFilter, err = filter.Compile(t.TagKeys)
	if err != nil {
		return errors.Errorf("fail to compile tag keys filter: %s", err)
	}
	return nil
}

func (t *Metaserver) gatherHost(acc telegraf.Accumulator) error {
	hostname, err := util.GetHostname(t.etcPath, t.procPath)
	if err != nil {
		return err
	}

	cpuTimes, err := cpu.Times(true)
	if err != nil {
		return errors.Errorf("fail to get cpu times: %s", err)
	}

	memStat, err := mem.VirtualMemory()
	if err != nil {
		return errors.Errorf("fail to get mem stat: %s", err)
	}

	diskStat, err := disk.Usage("/")
	if err != nil {
		return errors.Errorf("fail to get disk usage: %s", err)
	}

	platform, _, version, err := host.PlatformInformation()
	if err != nil {
		return errors.Errorf("fail to get platform info: %s", err)
	}

	kernelVersion, err := host.KernelVersion()
	if err != nil {
		return errors.Errorf("fail to get kernel version: %s", err)
	}

	var labels string
	lbs, err := node.GetLabels()
	if err == nil {
		labels = lbs.Line()
	}

	info := node.GetInfo()
	fields := map[string]interface{}{
		"cluster_full_name": info.ClusterName(),
		"private_addr":      info.HostIP(),
		"hostname":          hostname,
		"cpus":              len(cpuTimes),
		"memory":            memStat.Total,
		"disk":              diskStat.Total,
		"os":                platform + " " + version,
		"kernel_version":    kernelVersion,
		"system_time":       time.Now().String(),
		"labels":            labels,
		"timestamp":         time.Now().UnixNano(),
	}

	log.Printf("I! gather host fields: %v", fields)

	acc.AddFields(measurementHost, fields, map[string]string{})

	return nil
}

func (t *Metaserver) gatherAllRunningContainers(acc telegraf.Accumulator) error {
	var allContainers []string
	var hostIP string
	var ok bool

	client, timeout := docker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	containers, err := client.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return errors.Errorf("get docker container list: %s", err)
	}

	containersFields := make(map[string]interface{})
	for _, container := range containers {
		id := container.ID
		fields, _, err := t.getContainerFieldsAndTags(container.ID, containerStatusStarting)
		if err != nil {
			log.Printf("fail to get docker container: %s", err)
			continue
		}

		image := fields["image"].(string)
		image = t.getValidImage(image)
		if image == "" {
			continue
		}
		fields["image"] = image

		if hostIP, ok = fields["ip"].(string); !ok {
			return errors.Errorf("the type of ip is not string")
		}

		body, err := json.Marshal(fields)
		if err != nil {
			return errors.Errorf("json marshal container fields: %v", err)
		}

		containersFields[id] = body
		allContainers = append(allContainers, id)
	}

	log.Printf("I! first push all running containers: %v", allContainers)

	acc.AddFields(measurementAllRunningContainers, containersFields, map[string]string{"all_running_containers": hostIP})

	return nil
}

func (t *Metaserver) gatherContainerByDockerEvents(acc telegraf.Accumulator) error {
	log.Printf("I! Start to gather container from docker events!")
	defer log.Printf("I! End to gather container from docker events!")

	client, _ := docker.GetClient()

	t.watcherStarted = true
	defer func() {
		t.watcherStarted = false
	}()

	// since now
	options := types.EventsOptions{
		Filters: filters.NewArgs(initialFilterArgs...),
		Since:   time.Now().Format(time.RFC3339),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, errs := client.Events(ctx, options)

	for {
		select {
		case event := <-events:
			log.Printf("I! recv docker event message: %+v", event)
			if err := t.handleEvent(event, acc); err != nil {
				log.Printf("W! failed to handle docker event, id: %s, type: %s, action: %s, error: %s",
					event.ID, event.Type, event.Action, err)
			}
		case err := <-errs:
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

type containerStatus string

const (
	containerStatusStarting containerStatus = "Starting"
	containerStatusKilled   containerStatus = "Killed"
	containerStatusOOM      containerStatus = "OOM"
	containerStatusStopped  containerStatus = "Stopped"
)

func (t *Metaserver) handleEvent(event events.Message, acc telegraf.Accumulator) error {
	var status containerStatus

	if event.ID == "" || event.Type != events.ContainerEventType {
		return errors.Errorf("invalid docker event, container id is null")
	}

	switch event.Action {
	case dockerEventActionStart:
		status = containerStatusStarting
	case dockerEventActionKill:
		status = containerStatusKilled
	case dockerEventActionOOM:
		status = containerStatusOOM
	case dockerEventActionDie:
		status = containerStatusStopped
	default:
		return errors.Errorf("invalid docker event action: %s", event.Action)
	}

	return t.gatherContainer(acc, event.ID, status)
}

func (t *Metaserver) getContainerFieldsAndTags(id string, status containerStatus) (
	map[string]interface{}, map[string]string, error) {
	client, timeout := docker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	inspect, err := client.ContainerInspect(ctx, id)
	if err != nil {
		return nil, nil, errors.Errorf("inspect docker container %s: %v", id, err)
	}

	var ip string
	for _, key := range t.DockerNetworks {
		if inspect.NetworkSettings.Networks[key] != nil {
			ip = inspect.NetworkSettings.Networks[key].IPAddress
			break
		}
	}

	image := inspect.Image
	if strings.HasPrefix(image, "sha") {
		imageInspect, _, err := client.ImageInspectWithRaw(ctx, image)
		if err != nil {
			log.Printf("W! failed to get docker image, id: %s, error: %s", image, err)
		} else if len(imageInspect.RepoTags) == 0 {
			image = inspect.Config.Image
		} else {
			image = imageInspect.RepoTags[0]
		}
	}

	info := node.GetInfo()

	// TODO: CMDB 调整 status 字段，结合业务层定义的状态
	fields := make(map[string]interface{})
	fields = map[string]interface{}{
		"id":           id,
		"ip":           ip,
		"cluster_name": info.ClusterName(),
		"host_ip":      info.HostIP(),
		"started_at":   inspect.Created,
		"finished_at":  inspect.State.FinishedAt, // if running, 0001-01-01T00:00:00Z
		"image":        image,
		"cpu":          float64(inspect.HostConfig.CPUShares) / float64(1024),
		"memory":       inspect.HostConfig.Memory,
		// "disk":         inspect.HostConfig.DiskQuota,
		"exit_code":  inspect.State.ExitCode,
		"privileged": inspect.HostConfig.Privileged,
		"status":     inspect.State.Status, // String representation of the container state. Can be one of "created", "running", "paused", "restarting", "removing", "exited", or "dead"
		"timestamp":  time.Now().UnixNano(),
	}

	// 默认 status = running
	if status != "" {
		fields["status"] = string(status)
	} else {
		fields["status"] = string(containerStatusStarting)
	}

	if inspect.State.OOMKilled {
		fields["status"] = string(containerStatusOOM)
	}

	tags := map[string]string{
		"id": id,
	}
	for _, env := range inspect.Config.Env {
		kv := strings.Split(env, "=")
		if len(kv) != 2 {
			continue
		}

		key, value := kv[0], kv[1]
		if ip == "" && t.IPKey != "" && t.IPKey == key {
			fields["ip"] = value
			continue
		}
		if t.fieldKeysFilter.Match(key) {
			fields[strings.ToLower(key)] = value
		}
		if t.tagKeysFilter.Match(key) {
			tags[key] = value
		}
	}
	for key, value := range inspect.Config.Labels {
		if ip == "" && t.IPKey != "" && t.IPKey == key {
			fields["ip"] = value
			continue
		}

		if key == mesosTaskIDKey {
			fields["task_id"] = value
			tags["task_id"] = value
			continue
		}

		if t.fieldKeysFilter.Match(key) {
			fields[strings.ToLower(key)] = value
		}
		if t.tagKeysFilter.Match(key) {
			tags[key] = value
		}
	}

	if _, ok := fields["task_id"]; !ok {
		fields["task_id"] = id
		tags["task_id"] = id
	}
	if _, ok := fields["ip"]; !ok {
		fields["ip"] = fields["host_ip"]
	}

	return fields, tags, nil
}

func (t *Metaserver) gatherContainer(acc telegraf.Accumulator, id string, status containerStatus) error {
	fields, tags, err := t.getContainerFieldsAndTags(id, status)
	if err != nil {
		return err
	}

	image := fields["image"].(string)
	image = t.getValidImage(image)
	if image == "" {
		return nil
	}
	fields["image"] = image

	// log.Printf("gather container fields: %+v", fields)
	// log.Printf("gather container tags: %+v", tags)

	acc.AddFields(measurementContainer, fields, tags)
	return nil
}

func (t *Metaserver) pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	log.Printf("E stat %s error: %s", path, err)
	return false
}

func (t *Metaserver) getValidImage(image string) string {
	if strings.Contains(image, "kubernetes/pause") {
		return ""
	}
	return image
}

func init() {
	inputs.Add("metaserver", func() telegraf.Input {
		return &Metaserver{
			init:                     false,
			watcherStarted:           false,
			pushFullContainerStarted: false,
			pushHostsStarted:         false,
			pushPeriod:               15 * time.Minute,
		}
	})
}
