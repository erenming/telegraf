package dockersummary

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	apiv1 "k8s.io/api/core/v1"
)

const (
	labelKubernetesPodName       = "io.kubernetes.pod.name"
	labelKubernetesPodNamespace  = "io.kubernetes.pod.namespace"
	labelKubernetesPodUID        = "io.kubernetes.pod.uid"
	labelKubernetesType          = "io.kubernetes.docker.type"
	labelKubernetesContainerName = "io.kubernetes.container.name"
)

func (s *Summary) getPodContainer(containerID string, id kubernetes.PodId) (pc apiv1.Container, ok bool) {
	if !strings.HasPrefix(containerID, "docker://") {
		containerID = "docker://" + containerID
	}

	pmap, ok := kubernetes.GetPodMap()
	if !ok {
		return apiv1.Container{}, false
	}
	pod, ok := pmap.Load(id)
	if !ok {
		return apiv1.Container{}, false
	}

	return s.getContainerSpecById(containerID, pod.(*apiv1.Pod))
}

func (s *Summary) getContainerSpecById(containerID string, pod *apiv1.Pod) (pc apiv1.Container, ok bool) {
	if len(pod.Spec.Containers) != len(pod.Status.ContainerStatuses) {
		return apiv1.Container{}, false
	}

	index := -1
	for i := 0; i < len(pod.Status.ContainerStatuses); i++ {
		pcs := pod.Status.ContainerStatuses[i]
		if pcs.ContainerID == containerID && csIsRunning(pcs) {
			index = i
			break
		}
	}

	if index != -1 {
		return pod.Spec.Containers[index], true
	}
	return apiv1.Container{}, false
}

func csIsRunning(cs apiv1.ContainerStatus) bool {
	if cs.State.Running != nil {
		return true
	}
	return false
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
