package node

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf/plugins/inputs/global/kubernetes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *collector) populateWithK8S() {
	client, timeout := kubernetes.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("E! fail to get nodes error: %v", err)
	}
	log.Printf("I! input [node] get nodes list ok")
	for _, node := range nodeList.Items {

		for _, addr := range node.Status.Addresses {
			if addr.Address != "" && addr.Type == "InternalIP" && addr.Address == c.hostIP && node.ObjectMeta.Name != "" {
				log.Printf("get k8s node: %s", c.nodeName)
				c.nodeName = node.ObjectMeta.Name
				c.allocatable = getAllocatable(node)
			}
		}

	}
}

func getAllocatable(node v1.Node) map[string]float64 {
	all := node.Status.Allocatable
	res := make(map[string]float64, len(all))
	for k, v := range all {
		res[string(k)] = convertQuantityFloat(v.String(), 1)
	}
	return res
}

func (c *collector) getK8sNodeLabels(name string) (string, map[string]string, error) {
	client, timeout := kubernetes.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	node, err := client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		log.Printf("E! fail to get node labels from %s error: %v", name, err)
		return "", nil, err
	}
	log.Printf("I! input [node] get node %s labels ok", name)
	sb := strings.Builder{}
	sb.WriteString("K8S_ATTRIBUTES=dice_tags:")
	labelsMap := make(map[string]string)
	if node.ObjectMeta.Labels != nil {
		// MESOS_ATTRIBUTES=dice_tags:any,org-terminus,workspace-dev,workspace-test,workspace-staging,workspace-prod
		// node.Metadata.Labels = map[string]string{
		// 	"dice/any":               "",
		// 	"dice/org-terminus":      "",
		// 	"dice/workspace-dev":     "",
		// 	"dice/workspace-test":    "",
		// 	"dice/workspace-staging": "",
		// 	"dice/workspace-prod":    "",
		// }
		var find bool
		for key, v := range node.ObjectMeta.Labels {
			if strings.HasPrefix(key, "dice/") {
				sb.WriteString(key[len("dice/"):])
				sb.WriteString(",")
				labelsMap[key[len("dice/"):]] = v
				find = true
			}
		}
		if find {
			labels := sb.String()
			return labels[0 : len(labels)-1], labelsMap, nil
		}
	}
	return sb.String(), labelsMap, nil
}

func convertQuantityFloat(s string, m float64) float64 {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		log.Printf("E! Failed to parse quantity - %v", err)
		return 0
	}
	f, err := strconv.ParseFloat(fmt.Sprint(q.AsDec()), 64)
	if err != nil {
		log.Printf("E! Failed to parse float - %v", err)
		return 0
	}
	if m < 1 {
		m = 1
	}
	return f * m
}
