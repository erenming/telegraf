package dockersummary

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf/plugins/inputs/global/kubelet"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	labelKubernetesPodName       = "io.kubernetes.pod.name"
	labelKubernetesPodNamespace  = "io.kubernetes.pod.namespace"
	labelKubernetesPodUID        = "io.kubernetes.pod.uid"
	labelKubernetesType          = "io.kubernetes.docker.type"
	labelKubernetesContainerName = "io.kubernetes.container.name"
)

func (s *Summary) getPodContainer(containerID string, id kubelet.PodID) (pc *kubelet.PodContainer, ok bool) {
	if !strings.HasPrefix(containerID, "docker://") {
		containerID = "docker://" + containerID
	}

	pod, ok := s.pods[id]
	if !ok {
		return nil, false
	}

	return s.getContainerSpecById(containerID, pod)
}

func (s *Summary) getContainerSpecById(containerID string, pod *kubelet.PodInfo) (pc *kubelet.PodContainer, ok bool) {
	if len(pod.Spec.Containers) != len(pod.Status.ContainerStatuses) {
		return nil, false
	}

	cname := ""
	for i := 0; i < len(pod.Status.ContainerStatuses); i++ {
		pcs := pod.Status.ContainerStatuses[i]
		if pcs.ContainerID == containerID && csIsRunning(pcs) {
			cname = pcs.Name
			break
		}
	}

	for _, pc := range pod.Spec.Containers {
		if pc.Name == cname {
			return pc, true
		}
	}
	return nil, false
}

func csIsRunning(cs *kubelet.ContainerStatus) bool {
	_, ok := cs.State["running"]
	return ok
}

func convertQuantityFloat(s string, m float64) float64 {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	f, err := strconv.ParseFloat(fmt.Sprint(q.AsDec()), 64)
	if err != nil {
		return 0
	}
	if m < 1 {
		m = 1
	}
	return f * m
}

type kmemStats struct {
	UsageInBytes uint64
}

type containerCGroupKernelInfo struct {
	memoryStats *kmemStats
}

func (s *Summary) getContainerKernelInfo(podUID string, containerID string) *containerCGroupKernelInfo {
	res := &containerCGroupKernelInfo{}
	containerCGroupPath := filepath.Join(s.HostMountPrefix, "/sys/fs/cgroup/memory/kubepods/burstable", fmt.Sprintf("/pod%s/%s", podUID, containerID))

	res.memoryStats = getKmem(containerCGroupPath)
	return res
}

func getKmem(path string) *kmemStats {
	res := &kmemStats{}
	if v, err := ioutil.ReadFile(filepath.Join(path, "/memory.kmem.usage_in_bytes")); err == nil {
		if val, err := strconv.ParseUint(strings.Trim(string(v), "\n"), 10, 64); err == nil {
			res.UsageInBytes = val
		} else {
			log.Printf("D! ==== kmem path: %s; value: %s.\n", filepath.Join(path, "/memory.kmem.usage_in_bytes"), string(v))
		}
	}
	return res
}
