package kubernetes

import (
	"context"
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
			return
		case event, ok := <-pv.watcher.ResultChan():
			if !ok {
				return
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				pod := event.Object.(*apiv1.Pod)
				pv.pods.Store(GetPodID(pod.Name, pod.Namespace), pod)
			case watch.Deleted:
				pod := event.Object.(*apiv1.Pod)
				pv.pods.Delete(GetPodID(pod.Name, pod.Namespace))
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
