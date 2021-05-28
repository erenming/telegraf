package kubernetes

import (
	"context"
	"testing"
	"time"

	tk8s "github.com/influxdata/telegraf/plugins/common/kubernetes"
)

func TestViewing(t *testing.T) {
	cli, err := tk8s.NewClient(tk8s.Config{})
	if err != nil {
		t.Fatal()
	}
	pv := NewPodViewer(tk8s.NewWPodWatcher(context.TODO(), cli, tk8s.Selector{
		Namespace:     "",
		LabelSelector: "",
		FieldSelector: "metadata.namespace!=kuberhealthy,status.phase=Running,spec.nodeName=node-010000007117",
	}))
	go func() {
		for {
			time.Sleep(time.Second * 3)
			pv.pods.Range(func(key, value interface{}) bool {
				t.Logf("key: %s", key)
			})
		}
	}()
	pv.Viewing(context.TODO())
}
