package kube_inventory

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 获取一些特殊的 labels
func getLabels(meta metav1.ObjectMeta, tags map[string]string) {
	// 获取offline标
	if meta.Labels["dice/offline"] == "true" || meta.Labels["offline"] == "true" {
		tags["offline"] = "true"
	}

	if cmp, ok := meta.Labels["dice/component"]; ok {
		tags["component_name"] = cmp
	}
}
