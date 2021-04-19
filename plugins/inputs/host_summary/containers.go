package hostsummary

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/global/docker"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubelet"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ContainersCollector .
type ContainersCollector struct{}

// Gather .
func (c *ContainersCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	info := node.GetInfo()
	k8s := info.IsK8s()
	c.getContainers(tags, fields, acc, k8s)
	if k8s {
		// k8s 通过 kubelet 统计机器资源使用
		c.getPodsResource(tags, fields, acc)
	}
	return nil
}

func (c *ContainersCollector) getContainers(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator, k8s bool) error {
	client, timeout := docker.GetClient()
	if client == nil {
		return nil
	}
	filterArgs := filters.NewArgs()
	filterArgs.Add("status", "running")
	opts := types.ContainerListOptions{
		Filters: filterArgs,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	containers, err := client.ContainerList(ctx, opts)
	if err != nil {
		log.Printf("fail to get container list: %s", err)
		return err
	}
	fields["containers"] = len(containers)
	var (
		tasks int
		total containerResource
	)
	for _, container := range containers {
		// 过滤 k8s pause 容器
		if container.Labels["io.kubernetes.container.name"] == "POD" &&
			container.Labels["io.kubernetes.docker.type"] == "podsandbox" {
			continue
		}
		// if !strings.HasPrefix(c.Image, "sha256") {
		// 	if strings.Contains(c.Image, "kubernetes/pause") {
		// 		continue
		// 	} else if strings.Contains(c.Image, "dice-third-party/pause") {
		// 		continue
		// 	}
		// }
		tasks++
		if !k8s {
			// dcos 还是按 环境变量方式统计机器 资源
			res, err := c.getContainerResource(container.ID)
			if err != nil {
				log.Printf("fail to inspect container %s: %s", container.ID, err)
				continue
			}
			total.cpuRequest += res.cpuRequest
			total.cpuLimit += res.cpuLimit
			total.cpuOrigin += res.cpuOrigin
			total.memRequest += res.memRequest
			total.memLimit += res.memLimit
			total.memOrigin += res.memOrigin
		}
	}
	fields["task_containers"] = tasks
	if !k8s {
		fields["cpu_limit_total"] = total.cpuLimit
		fields["cpu_request_total"] = total.cpuRequest
		fields["cpu_origin_total"] = total.cpuOrigin
		fields["mem_limit_total"] = total.memLimit
		fields["mem_request_total"] = total.memRequest
		fields["mem_origin_total"] = total.memOrigin
	}
	return nil
}

func (c *ContainersCollector) getPodsResource(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	pods, err := kubelet.GetPods()
	if err != nil {
		log.Printf("fail to get pods : %s", err)
		return err
	}
	var (
		memReq, memLimit int64
		cpuReq, cpuLimit int64
	)
	for _, pod := range pods {
		if pod.Status.Phase != "Running" {
			continue
		}

		for i := 0; i < len(pod.Spec.Containers); i++ {
			c := pod.Spec.Containers[i]
			req := c.Resources.Requests
			lim := c.Resources.Limits
			memReq += convertQuantity(req.Memory, 1)
			memLimit += convertQuantity(lim.Memory, 1)
			cpuReq += convertQuantity(req.CPU, 1000)
			cpuLimit += convertQuantity(lim.CPU, 1000)
		}
	}
	fields["cpu_limit_total"] = float64(cpuLimit) / 1000
	fields["cpu_request_total"] = float64(cpuReq) / 1000
	fields["cpu_origin_total"] = float64(cpuReq) / 1000
	fields["mem_limit_total"] = memLimit
	fields["mem_request_total"] = memReq
	fields["mem_origin_total"] = memReq
	return nil
}

func convertQuantity(s string, m float64) int64 {
	if len(s) <= 0 {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		log.Printf("E! Failed to parse quantity %s - %v", s, err)
		return 0
	}
	f, err := strconv.ParseFloat(fmt.Sprint(q.AsDec()), 64)
	if err != nil {
		log.Printf("E! Failed to parse float - %v", err)
		return 0
	}
	if m < 1 {
		m = 1
	}
	return int64(f * m)
}

type containerResource struct {
	cpuRequest, cpuLimit, cpuOrigin float64
	memRequest, memLimit, memOrigin float64
}

func (c *ContainersCollector) getContainerResource(id string) (*containerResource, error) {
	client, timeout := docker.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	inspect, err := client.ContainerInspect(ctx, id)
	if err != nil {
		return nil, err
	}
	var res containerResource
	for _, line := range inspect.Config.Env {
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		key := line[0:idx]
		val := line[idx+1:]
		switch key {
		case "DICE_CPU_REQUEST":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.cpuRequest = v
			}
		case "DICE_CPU_LIMIT":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.cpuLimit = v
			}
		case "DICE_CPU_ORIGIN":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.cpuOrigin = v
			}
		case "DICE_MEM_REQUEST":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.memRequest = v * 1024 * 1024
			}
		case "DICE_MEM_LIMIT":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.memLimit = v * 1024 * 1024
			}
		case "DICE_MEM_ORIGIN":
			v, err := strconv.ParseFloat(val, 64)
			if err == nil {
				res.memOrigin = v * 1024 * 1024
			}
		}
	}
	return &res, nil
}

func init() {
	RegisterCollector("containers", func(map[string]interface{}) Collector {
		return &ContainersCollector{}
	})
}
