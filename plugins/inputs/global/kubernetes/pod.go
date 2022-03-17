package kubernetes

import (
	"context"
	"log"
	"sync"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

type Viewer interface {
	Viewing(ctx context.Context)
	GetData() *sync.Map
}

type PodId string

type podViewer struct {
	watcher watch.Interface
	pods    *sync.Map
}

func NewPodViewer(w watch.Interface) Viewer {
	return &podViewer{
		watcher: w,
		pods:    &sync.Map{},
	}
}

func (pv *podViewer) Viewing(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			pv.watcher.Stop()
			return
		case event := <-pv.watcher.ResultChan():
			pod, ok := event.Object.(*apiv1.Pod)
			if !ok {
				continue
			}
			id := GetPodID(pod.Name, pod.Namespace)
			switch event.Type {
			// id := GetPodID(pod.Name, pod.Namespace)
			case watch.Added, watch.Modified:
				pv.pods.Store(id, pod)
			case watch.Deleted:
				pv.pods.Delete(id)
			default:
			}
		}
	}
}

func (pv *podViewer) GetData() *sync.Map {
	return pv.pods
}

func GetPodID(name, namespace string) PodId {
	return PodId(namespace + "/" + name)
}

func GetPodMap() (*sync.Map, bool) {
	obj, ok := instance.viewers[resourcePod]
	if !ok {
		return nil, false
	}
	return obj.GetData(), true
}
