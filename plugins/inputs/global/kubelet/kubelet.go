package kubelet

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 只定义需要获取的信息，解析时可以节省内存
type (
	// PodID .
	PodID struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	// ResourceLimit .
	ResourceLimit struct {
		CPU    string `json:"cpu"`
		Memory string `json:"memory"`
	}
	// ResourcesRequest .
	ResourcesRequest struct {
		CPU    string `json:"cpu"`
		Memory string `json:"memory"`
	}
	// Resources .
	Resources struct {
		Limits   ResourceLimit    `json:"limits"`
		Requests ResourcesRequest `json:"requests"`
	}
	// PodContainer .
	PodContainer struct {
		Name      string    `json:"name"`
		Resources Resources `json:"resources"`
	}
	// ContainerState .
	ContainerState struct {
		ExitCode    int    `json:"exitCode"`
		Reason      string `json:"reason"`
		StartedAt   string `json:"startedAt"`
		FinishedAt  string `json:"finishedAt"`
		ContainerID string `json:"containerID"`
	}
	// ContainerStatus .
	ContainerStatus struct {
		ContainerID  string                     `json:"containerID"`
		State        map[string]*ContainerState `json:"state"`
		RestartCount int                        `json:"restartCount"`
	}
	// PodInfo .
	// PodInfo struct {
	// 	MetaData struct {
	// 		PodID
	// 		Labels            map[string]string `json:"labels"`
	// 		CreationTimestamp string            `json:"creationTimestamp"`
	// 	} `json:"metadata"`
	// 	Spec struct {
	// 		NodeName   string          `json:"nodeName"`
	// 		Containers []*PodContainer `json:"containers"`
	// 	} `json:"spec"`
	// 	Status struct {
	// 		Phase             string             `json:"phase"`
	// 		Reason            string             `json:"reason"`
	// 		Message           string             `json:"message"`
	// 		ContainerStatuses []*ContainerStatus `json:"containerStatuses"`
	// 	} `json:"status"`
	// }
	// PodsResp .
	// PodsResp struct {
	// 	Items []*PodInfo
	// }
	// Network .
	Network struct {
		Name     string `json:"name"`
		RxBytes  int64  `json:"rxBytes"`
		RxErrors int64  `json:"rxErrors"`
		TxBytes  int64  `json:"txBytes"`
		TxErrors int64  `json:"txErrors"`
	}
	// PodStatus .
	PodStatus struct {
		PodRef  PodID   `json:"podRef"`
		Network Network `json:"network"`
	}
	// StatusSummaryResp .
	StatusSummaryResp struct {
		Pods []*PodStatus `json:"pods"`
	}
)

// kubelet .
type kubelet struct {
	global.Base
	GatherPods       bool `toml:"gather_pods"`
	client           *http.Client
	lastGetPods      time.Time
	lastGetPodStatus time.Time
	lock             sync.Mutex
	podsStats        map[PodID]*PodStatus
}

func (k *kubelet) Gather(acc telegraf.Accumulator) error {
	info := node.GetInfo()
	if !info.IsK8s() {
		return nil
	}
	pods, ok := kubernetes.GetPodMap()
	if !ok {
		return fmt.Errorf("kubernetes.GetPodMap faile")
	}

	pods.Range(func(key, value interface{}) bool {
		p, ok := value.(*apiv1.Pod)
		if !ok {
			return true
		}
		gatherPodStatus(p, acc)
		for i, cs := range p.Status.ContainerStatuses {
			c := p.Spec.Containers[i]
			gatherPodContainer(p.Spec.NodeName, p, cs, c, acc)
		}
		return true
	})

	return nil
}

func (k *kubelet) getStatsSummary() (map[PodID]*PodStatus, error) {
	info := node.GetInfo()
	url := fmt.Sprintf("http://%s:10255/stats/summary", info.HostIP())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := k.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s Received status code %d (%s), expected %d (%s)",
			url,
			resp.StatusCode,
			http.StatusText(resp.StatusCode),
			http.StatusOK,
			http.StatusText(http.StatusOK))
	}
	stats := StatusSummaryResp{}
	err = json.NewDecoder(resp.Body).Decode(&stats)
	if err != nil {
		return nil, fmt.Errorf("%s fail to json.Unmarshal: %s", url, err)
	}
	pods := make(map[PodID]*PodStatus)
	for _, pod := range stats.Pods {
		pods[pod.PodRef] = pod
	}
	return pods, nil
}

func (k *kubelet) GetStatsSummary() (map[PodID]*PodStatus, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	now := time.Now()
	if k.lastGetPodStatus.Add(20*time.Second).After(now) && k.podsStats != nil {
		log.Println("I! [kubectl] get pods status from cache")
		return k.podsStats, nil
	}
	podsStats, err := k.getStatsSummary()
	if err != nil {
		return nil, err
	}
	k.podsStats = podsStats
	k.lastGetPodStatus = now
	return podsStats, nil
}

var instance = &kubelet{
	client: &http.Client{Timeout: time.Second * 8},
}

func init() {
	inputs.Add("global_kubelet", func() telegraf.Input {
		return instance
	})
}

func gatherPodStatus(p *apiv1.Pod, acc telegraf.Accumulator) {
	fields := map[string]interface{}{
		"status":  p.Status.Phase,
		"reason":  p.Status.Reason,
		"message": p.Status.Message,
	}
	tags := map[string]string{
		"namespace": p.ObjectMeta.Namespace,
		"node_name": p.Spec.NodeName,
		"pod_name":  p.ObjectMeta.Name,
	}
	getLabels(p.ObjectMeta.Labels, fields, tags)
	acc.AddFields("kubernetes_pod_status", fields, tags)
}

func gatherPodContainer(nodeName string, p *apiv1.Pod, cs apiv1.ContainerStatus, c apiv1.Container, acc telegraf.Accumulator) {
	stateCode, state := 3, "unknown"

	var reason string
	if cs.State.Running != nil {
		stateCode, state = 0, "running"
	} else if cs.State.Terminated != nil {
		stateCode, state = 1, "terminated"
		reason = cs.State.Terminated.Reason
	} else if cs.State.Waiting != nil {
		stateCode, state = 2, "waiting"
	}

	fields := map[string]interface{}{
		"restarts_total":    cs.RestartCount,
		"state_code":        stateCode,
		"terminated_reason": reason,
	}
	tags := map[string]string{
		"container_name": c.Name,
		"namespace":      p.ObjectMeta.Namespace,
		"node_name":      p.Spec.NodeName,
		"pod_name":       p.ObjectMeta.Name,
		"state":          state,
	}

	req := c.Resources.Requests
	fields["resource_requests_millicpu_units"] = convertQuantity(req.Cpu().String(), 1000)
	fields["resource_requests_memory_bytes"] = convertQuantity(req.Memory().String(), 1)
	lim := c.Resources.Limits
	fields["resource_limits_millicpu_units"] = convertQuantity(lim.Cpu().String(), 1000)
	fields["resource_limits_memory_bytes"] = convertQuantity(lim.Memory().String(), 1)

	getLabels(p.ObjectMeta.Labels, fields, tags)

	acc.AddFields("kubernetes_pod_container", fields, tags)
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

// 获取一些特殊的 labels
func getLabels(labels map[string]string, fields map[string]interface{}, tags map[string]string) {
	// 获取offline标
	if labels["dice/offline"] == "true" || labels["offline"] == "true" {
		tags["offline"] = "true"
	}
	if cmp, ok := labels["dice/component"]; ok {
		tags["component_name"] = cmp
	}
}

// GetStatsSummery .
func GetStatsSummery() (map[PodID]*PodStatus, error) {
	return instance.GetStatsSummary()
}
