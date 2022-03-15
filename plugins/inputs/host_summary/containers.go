package hostsummary

import (
	"fmt"
	"log"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ContainersCollector .
type ContainersCollector struct{}

// Gather .
func (c *ContainersCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	if !node.GetInfo().IsK8s() {
		return nil
	}
	if err := c.getPodsResource(tags, fields, acc); err != nil {
		return fmt.Errorf("getPodsResource err: %w", err)
	}
	return nil
}

// deprecated. Dcos not supported
// func (c *ContainersCollector) getContainers(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator, k8s bool) error {
// 	client, timeout := docker.GetClient()
// 	if client == nil {
// 		return nil
// 	}
// 	filterArgs := filters.NewArgs()
// 	filterArgs.Add("status", "running")
// 	opts := types.ContainerListOptions{
// 		Filters: filterArgs,
// 	}
// 	ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 	defer cancel()
// 	containers, err := client.ContainerList(ctx, opts)
// 	if err != nil {
// 		log.Printf("fail to get container list: %s", err)
// 		return err
// 	}
// 	fields["containers"] = len(containers)
// 	var (
// 		tasks int
// 		total containerResource
// 	)
// 	for _, container := range containers {
// 		// 过滤 k8s pause 容器
// 		if container.Labels["io.kubernetes.container.name"] == "POD" &&
// 			container.Labels["io.kubernetes.docker.type"] == "podsandbox" {
// 			continue
// 		}
// 		// if !strings.HasPrefix(c.Image, "sha256") {
// 		// 	if strings.Contains(c.Image, "kubernetes/pause") {
// 		// 		continue
// 		// 	} else if strings.Contains(c.Image, "dice-third-party/pause") {
// 		// 		continue
// 		// 	}
// 		// }
// 		tasks++
// 		if !k8s {
// 			// dcos 还是按 环境变量方式统计机器 资源
// 			res, err := c.getContainerResource(container.ID)
// 			if err != nil {
// 				log.Printf("fail to inspect container %s: %s", container.ID, err)
// 				continue
// 			}
// 			total.cpuRequest += res.cpuRequest
// 			total.cpuLimit += res.cpuLimit
// 			total.cpuOrigin += res.cpuOrigin
// 			total.memRequest += res.memRequest
// 			total.memLimit += res.memLimit
// 			total.memOrigin += res.memOrigin
// 		}
// 	}
// 	fields["task_containers"] = tasks
// 	if !k8s {
// 		fields["cpu_limit_total"] = total.cpuLimit
// 		fields["cpu_request_total"] = total.cpuRequest
// 		fields["cpu_origin_total"] = total.cpuOrigin
// 		fields["mem_limit_total"] = total.memLimit
// 		fields["mem_request_total"] = total.memRequest
// 		fields["mem_origin_total"] = total.memOrigin
// 	}
// 	return nil
// }

// refer https://github.com/kubernetes-client/python/issues/651
// accumulate: /pods?fieldSelector=spec.nodeName=node-xxx,status.phase!=Failed,status.phase!=Succeeded
func (c *ContainersCollector) getPodsResource(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	pods, ok := kubernetes.GetPodMap()
	if !ok {
		log.Printf("E! fail to GetPodMap")
		return nil
	}
	var (
		memReq, memLimit  int64
		cpuReq, cpuLimit  float64
		containers, tasks int
	)

	pods.Range(func(key, value interface{}) bool {
		pod, ok := value.(*apiv1.Pod)
		if !ok {
			return true
		}

		if pod.Status.Phase == apiv1.PodFailed || pod.Status.Phase == apiv1.PodSucceeded {
			return true
		}

		containers++ // count pause container
		for _, c := range pod.Status.ContainerStatuses {
			if c.State.Running != nil {
				containers++
				tasks++
			}
		}

		for i := 0; i < len(pod.Spec.Containers); i++ {
			c := pod.Spec.Containers[i]
			req := c.Resources.Requests
			lim := c.Resources.Limits
			memReq += req.Memory().Value()
			memLimit += lim.Memory().Value()
			cpuReq += req.Cpu().AsApproximateFloat64()
			cpuLimit += lim.Cpu().AsApproximateFloat64()
		}
		return true
	})

	fields["cpu_limit_total"] = cpuLimit
	fields["cpu_request_total"] = cpuReq
	fields["cpu_origin_total"] = cpuReq
	fields["mem_limit_total"] = memLimit
	fields["mem_request_total"] = memReq
	fields["mem_origin_total"] = memReq
	fields["containers"] = containers
	fields["task_containers"] = tasks
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

// func (c *ContainersCollector) getContainerResource(id string) (*containerResource, error) {
// 	client, timeout := docker.GetClient()
// 	ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 	defer cancel()
// 	inspect, err := client.ContainerInspect(ctx, id)
// 	if err != nil {
// 		return nil, err
// 	}
// 	var res containerResource
// 	for _, line := range inspect.Config.Env {
// 		idx := strings.Index(line, "=")
// 		if idx < 0 {
// 			continue
// 		}
// 		key := line[0:idx]
// 		val := line[idx+1:]
// 		switch key {
// 		case "DICE_CPU_REQUEST":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.cpuRequest = v
// 			}
// 		case "DICE_CPU_LIMIT":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.cpuLimit = v
// 			}
// 		case "DICE_CPU_ORIGIN":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.cpuOrigin = v
// 			}
// 		case "DICE_MEM_REQUEST":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.memRequest = v * 1024 * 1024
// 			}
// 		case "DICE_MEM_LIMIT":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.memLimit = v * 1024 * 1024
// 			}
// 		case "DICE_MEM_ORIGIN":
// 			v, err := strconv.ParseFloat(val, 64)
// 			if err == nil {
// 				res.memOrigin = v * 1024 * 1024
// 			}
// 		}
// 	}
// 	return &res, nil
// }

func init() {
	RegisterCollector("containers", func(map[string]interface{}) Collector {
		return &ContainersCollector{}
	})
}
