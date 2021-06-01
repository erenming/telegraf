package kubernetes

import (
	"context"
	"log"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	tk8s "github.com/influxdata/telegraf/plugins/common/kubernetes"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
	k8s "k8s.io/client-go/kubernetes"
)

const (
	resourcePod     kind = "pod"
	resourceService kind = "service"
)

// kubernetes .
type kubernetes struct {
	global.Base
	client *k8s.Clientset
	// k8s apiserver config
	K8sConfig      tk8s.Config      `toml:"k8s_config"`
	// watch resource config
	WatchResources []ResourceConfig `toml:"watch_resources"`
	viewers        map[kind]Viewer
	cancelFunc     context.CancelFunc
}

type kind string

type ResourceConfig struct {
	Kind kind `toml:"kind"`
	tk8s.Selector
}

// Start .
func (k *kubernetes) Start(acc telegraf.Accumulator) error {
	client, err := tk8s.NewClient(k.K8sConfig)
	if err != nil {
		return err
	}
	k.client = client
	log.Printf("create k8s client success!!!")

	ctx, cancel := context.WithCancel(context.Background())
	k.cancelFunc = cancel

	for _, rc := range k.WatchResources {
		switch rc.Kind {
		case resourcePod:
			pv := NewPodViewer(tk8s.NewWPodWatcher(ctx, client, tk8s.Selector{
				Namespace:     rc.Namespace,
				LabelSelector: rc.LabelSelector,
				FieldSelector: rc.FieldSelector,
			}))
			go pv.Viewing(ctx)
			k.viewers[resourcePod] = pv
		case resourceService:
			// todo
		}
	}

	return nil
}

func (k *kubernetes) Stop() {
	k.cancelFunc()
}

var instance = &kubernetes{
	K8sConfig: tk8s.Config{
		Timeout: config.Duration(20 * time.Second),
	},
	viewers: map[kind]Viewer{},
}

func init() {
	inputs.Add("global_kubernetes", func() telegraf.Input {
		return instance
	})
}

// GetClient .
func GetClient() (*k8s.Clientset, time.Duration) {
	return instance.client, time.Duration(instance.K8sConfig.Timeout)
}
