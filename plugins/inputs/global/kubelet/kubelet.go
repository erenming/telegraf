package kubelet

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
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
	PodInfo struct {
		MetaData struct {
			PodID
			Labels            map[string]string `json:"labels"`
			CreationTimestamp string            `json:"creationTimestamp"`
		} `json:"metadata"`
		Spec struct {
			NodeName   string          `json:"nodeName"`
			Containers []*PodContainer `json:"containers"`
		} `json:"spec"`
		Status struct {
			Phase             string             `json:"phase"`
			Reason            string             `json:"reason"`
			Message           string             `json:"message"`
			ContainerStatuses []*ContainerStatus `json:"containerStatuses"`
		} `json:"status"`
	}
	// PodsResp .
	PodsResp struct {
		Items []*PodInfo
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
	pods             map[PodID]*PodInfo
	podsStats        map[PodID]*PodStatus

	KubeletPort int    `toml:"kubelet_port"`
	K8sToken    string `toml:"k8s_token"`
	K8sTlsCa    string `toml:"k8s_tls_ca"`
	inited      bool
}

func (k *kubelet) Gather(acc telegraf.Accumulator) error {
	info := node.GetInfo()
	if !info.IsK8s() {
		return nil
	}
	if !k.inited {
		k.client = k.CreateClient()
		k.inited = true
	}
	pods, err := k.GetPods()
	if err != nil {
		return err
	}
	for _, p := range pods {
		if len(p.MetaData.CreationTimestamp) <= 0 {
			continue
		}
		ct, err := time.Parse("2006-01-02T15:04:05Z", p.MetaData.CreationTimestamp)
		if err != nil || ct.IsZero() {
			continue
		}
		gatherPodStatus(p, acc)
		for i, cs := range p.Status.ContainerStatuses {
			c := p.Spec.Containers[i]
			gatherPodContainer(p.Spec.NodeName, p, cs, c, acc)
		}
	}
	return nil
}

func gatherPodStatus(p *PodInfo, acc telegraf.Accumulator) {
	fields := map[string]interface{}{
		"status":  p.Status.Phase,
		"reason":  p.Status.Reason,
		"message": p.Status.Message,
	}
	tags := map[string]string{
		"namespace": p.MetaData.Namespace,
		"node_name": p.Spec.NodeName,
		"pod_name":  p.MetaData.Name,
	}
	getLabels(p.MetaData.Labels, fields, tags)
	acc.AddFields("kubernetes_pod_status", fields, tags)
}

func gatherPodContainer(nodeName string, p *PodInfo, cs *ContainerStatus, c *PodContainer, acc telegraf.Accumulator) {
	stateCode, state := 3, "unknown"
	var reason string
	for name, status := range cs.State {
		name = strings.ToLower(name)
		switch name {
		case "running":
			stateCode, state = 0, "running"
		case "terminated":
			stateCode, state = 1, "terminated"
			reason = status.Reason
		case "waiting":
			stateCode, state = 2, "waiting"
		}
		break
	}

	fields := map[string]interface{}{
		"restarts_total":    cs.RestartCount,
		"state_code":        stateCode,
		"terminated_reason": reason,
	}
	tags := map[string]string{
		"container_name": c.Name,
		"namespace":      p.MetaData.Namespace,
		"node_name":      p.Spec.NodeName,
		"pod_name":       p.MetaData.Name,
		"state":          state,
	}

	req := c.Resources.Requests
	fields["resource_requests_millicpu_units"] = convertQuantity(req.CPU, 1000)
	fields["resource_requests_memory_bytes"] = convertQuantity(req.Memory, 1)
	lim := c.Resources.Limits
	fields["resource_limits_millicpu_units"] = convertQuantity(lim.CPU, 1000)
	fields["resource_limits_memory_bytes"] = convertQuantity(lim.Memory, 1)

	getLabels(p.MetaData.Labels, fields, tags)

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

func (k *kubelet) getPods() (map[PodID]*PodInfo, error) {
	info := node.GetInfo()
	url := fmt.Sprintf("https://%s:%d/pods", info.HostIP(), k.KubeletPort)
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
	podsResp := PodsResp{}
	err = json.NewDecoder(resp.Body).Decode(&podsResp)
	if err != nil {
		return nil, fmt.Errorf("%s fail to json.Unmarshal: %s", url, err)
	}
	pods := make(map[PodID]*PodInfo)
	for _, pod := range podsResp.Items {
		pods[pod.MetaData.PodID] = pod
	}
	return pods, nil
}

func (k *kubelet) GetPods() (map[PodID]*PodInfo, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	now := time.Now()
	if k.lastGetPods.Add(20*time.Second).After(now) && k.pods != nil {
		log.Println("I! [kubectl] get pods from cache")
		return k.pods, nil
	}
	pods, err := k.getPods()
	if err != nil {
		return nil, err
	}
	k.pods = pods
	k.lastGetPods = now
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

// GetPods .
func GetPods() (map[PodID]*PodInfo, error) {
	return instance.GetPods()
}

// GetStatsSummery .
func GetStatsSummery() (map[PodID]*PodStatus, error) {
	return instance.GetStatsSummary()
}
