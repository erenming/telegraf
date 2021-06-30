package kubernetes

import (
	"context"
	"log"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type PodWatcher struct {
	informer cache.SharedInformer
	store    cache.Store
	queue    *workqueue.Type
}

func NewWPodWatcher(ctx context.Context, c *k8s.Clientset, selector Selector) Watcher {
	pg := c.CoreV1().Pods(selector.Namespace)
	informer := cache.NewSharedInformer(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = selector.FieldSelector
			options.LabelSelector = selector.LabelSelector
			return pg.List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = selector.FieldSelector
			options.LabelSelector = selector.LabelSelector
			return pg.Watch(ctx, options)
		},
	}, &apiv1.Pod{}, 10*time.Minute)
	go informer.Run(ctx.Done())

	p := &PodWatcher{
		informer: informer,
		store:    informer.GetStore(),
		queue:    workqueue.NewNamed("pod"),
	}

	p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			p.enqueue(obj, AddEvent)
		},
		UpdateFunc: func(_, obj interface{}) {
			p.enqueue(obj, AddEvent)
		},
		DeleteFunc: func(obj interface{}) {
			p.enqueue(obj, DeleteEvent)
		},
	})
	return p
}

func (p *PodWatcher) enqueue(obj interface{}, state EventState) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}
	p.queue.Add(&Item{State: state, Key: key, Obj: obj})
}

func (p *PodWatcher) Watch(ctx context.Context, ch chan<- *Item) {
	defer p.queue.ShutDown()
	if !cache.WaitForCacheSync(ctx.Done(), p.informer.HasSynced) {
		if ctx.Err() != context.Canceled {
			log.Printf("I! pod informer unable to sync cache")
		}
		return
	}

	go func() {
		for p.process(ctx, ch) {
		}
	}()

	<-ctx.Done()
}

func (p *PodWatcher) process(ctx context.Context, ch chan<- *Item) bool {
	keyObj, quit := p.queue.Get()
	if quit {
		return false
	}
	defer p.queue.Done(keyObj)

	item, ok := keyObj.(*Item)
	if !ok {
		return true
	}

	key, ok := item.Key.(string)
	if !ok {
		return true
	}

	_, exist, err := p.store.GetByKey(key)
	if err != nil {
		log.Printf("E! get key %s, error: %s\n", key, err)
		return true
	}
	if !exist && item.State != DeleteEvent {
		log.Printf("key not existed in store. item: %+v", item)
		return true
	}

	select {
	case <-ctx.Done():
		return false
	case ch <- item:
		return true
	}
}
