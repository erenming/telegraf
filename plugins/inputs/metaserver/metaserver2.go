package metaserver

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
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
	containerMeasurement = "container_metaserver"
	hostMeasurement      = "host_metaserver"
)

type Metaserver2 struct {
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
func (t *Metaserver2) Description() string {
	return "Read metaserver of machine and docker"
}

// SampleConfig metaserver sample config
func (t *Metaserver2) SampleConfig() string {
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

// Gather metaserver gather func
func (t *Metaserver2) Gather(acc telegraf.Accumulator) error {
	var err error

	log.Println("I! start to metaserver gather.")

	if !t.init {
		if err = t.initServer(); err != nil {
			return err
		}

		t.init = true
	}

	if err := t.gatherAllRunningContainers(acc); err != nil {
		log.Printf("failed to gather all running containers: %v", err)
	}

	if err := t.gatherHost(acc); err != nil {
		log.Printf("failed to gather host info: %v", err)
	}

	return nil
}

func (t *Metaserver2) initServer() error {
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

func (t *Metaserver2) gatherHost(acc telegraf.Accumulator) error {
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

	acc.AddFields(hostMeasurement, fields, map[string]string{})

	return nil
}

func (t *Metaserver2) gatherAllRunningContainers(acc telegraf.Accumulator) error {
	client, timeout := docker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	containers, err := client.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return errors.Errorf("get docker container list: %s", err)
	}

	for _, container := range containers {
		fields, tags, err := t.getContainerFieldsAndTags(container.ID, containerStatusStarting)
		if err != nil {
			log.Printf("fail to get docker container: %s", err)
			continue
		}
		acc.AddFields(containerMeasurement, fields, tags)
	}

	return nil
}

func (t *Metaserver2) getContainerFieldsAndTags(id string, status containerStatus) (
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
		"exit_code":    inspect.State.ExitCode,
		"privileged":   inspect.HostConfig.Privileged,
		"status":       inspect.State.Status, // String representation of the container state. Can be one of "created", "running", "paused", "restarting", "removing", "exited", or "dead"
		"timestamp":    time.Now().UnixNano(),
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

func (t *Metaserver2) pathExists(path string) bool {
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

func init() {
	inputs.Add("metaserver2", func() telegraf.Input {
		return &Metaserver2{
			init:                     false,
			watcherStarted:           false,
			pushFullContainerStarted: false,
			pushHostsStarted:         false,
		}
	})
}
