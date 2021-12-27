package kubernetes

import (
	"context"
	"sync"

	tk8s "github.com/influxdata/telegraf/plugins/common/kubernetes"
	apiv1 "k8s.io/api/core/v1"
)

type Viewer interface {
	Viewing(ctx context.Context)
	GetData() *sync.Map
}

type PodId string

type podViewer struct {
	watcher tk8s.Watcher
	pods    *sync.Map
}

func NewPodViewer(w tk8s.Watcher) Viewer {
	return &podViewer{
		watcher: w,
		pods:   &sync.Map{},
	}
}

func (pv *podViewer) Viewing(ctx context.Context) {
	ch := make(chan *tk8s.Item)
	go pv.watcher.Watch(ctx, ch)
	pv.consume(ctx, ch)
}

func (pv *podViewer) GetData() *sync.Map {
	return pv.pods
}

func (pv *podViewer) consume(ctx context.Context, ch <-chan *tk8s.Item) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-ch:
			pod, ok := item.Obj.(*apiv1.Pod)
			if !ok {
				continue
			}
			id := GetPodID(pod.Name, pod.Namespace)
			if item.State == tk8s.DeleteEvent {
				pv.pods.Delete(id)
			} else {
				pv.pods.Store(id, pod)
			}
		}
	}
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
