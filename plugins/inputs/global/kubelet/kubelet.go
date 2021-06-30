package kubelet

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	apiv1 "k8s.io/api/core/v1"
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
	client           *httpClient
	lastGetPods      time.Time
	lastGetPodStatus time.Time
	lock             sync.Mutex
	podsStats        map[PodID]*PodStatus

	KubeletPort int    `toml:"kubelet_port"`
	K8sToken    string `toml:"k8s_token"`
	K8sTlsCa    string `toml:"k8s_tls_ca"`
	inited      bool
}

func (k *kubelet) Gather(acc telegraf.Accumulator) error {
	if !k.inited {
		k.client = k.CreateClient()
		k.inited = true
	}
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
	fields["resource_requests_millicpu_units"] = req.Cpu().MilliValue()
	fields["resource_requests_memory_bytes"] = req.Memory().Value()
	lim := c.Resources.Limits
	fields["resource_limits_millicpu_units"] = lim.Cpu().MilliValue()
	fields["resource_limits_memory_bytes"] = lim.Memory().Value()

	getLabels(p.ObjectMeta.Labels, fields, tags)

	acc.AddFields("kubernetes_pod_container", fields, tags)
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

func (k *kubelet) getStatsSummary() (map[PodID]*PodStatus, error) {
	info := node.GetInfo()
	url := fmt.Sprintf("https://%s:%d/stats/summary", info.HostIP(), k.KubeletPort)
	request, err := k.client.NewRequest("GET", url, nil)
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

const (
	k8sCa        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sToken     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	normalPort   = 10250
	readOnlyPort = 10255
)

var instance = &kubelet{
	K8sTlsCa:    k8sCa,
	K8sToken:    k8sToken,
	KubeletPort: normalPort,
}

func init() {
	inputs.Add("global_kubelet", func() telegraf.Input {
		return instance
	})
}

// GetStatsSummery .
func GetStatsSummery() (map[PodID]*PodStatus, error) {
	return instance.GetStatsSummary()
}
