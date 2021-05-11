package add_kubernetes_metadata

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
	"github.com/influxdata/telegraf/plugins/processors"
)

const k8sLabelPrefix = "k8s_node_label_"

type AddKubernetesMetadata struct {
	Labels      []string `toml:"labels"`
	// Annotations []string `toml:"annotations"`
}

func (a *AddKubernetesMetadata) SampleConfig() string {
	return ""
}

func (a *AddKubernetesMetadata) Description() string {
	return ""
}

func (a *AddKubernetesMetadata) Apply(in ...telegraf.Metric) []telegraf.Metric {
	node.GetInfo()
	labels, err := node.GetLabels()
	if err != nil {
		return in
	}
	for _, item := range in {
		for _, label := range a.Labels {
			if v, ok := labels.OtherMap()[label]; ok {
				item.AddTag(k8sLabelPrefix+label, v)
			}
		}
	}
	return in
}

func init() {
	processors.Add("add_kubernetes_metadata", func() telegraf.Processor {
		p := &AddKubernetesMetadata{}
		return p
	})
}
