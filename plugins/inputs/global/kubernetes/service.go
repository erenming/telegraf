package kubernetes

import (
	"context"
	"fmt"

	"github.com/influxdata/telegraf/filter"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type Service struct {
	Name      string
	Namespace string
	Address   string
	Protocol  string
	Port      int32
}

type ServiceFilter struct {
	NameInclude             []string `toml:"name_include"` // glob
	Namespace               string   `toml:"namespace"`
	Port                    int32    `toml:"port"`
	Protocol                string   `toml:"schema"`
	KubernetesLabelSelector string   `toml:"kubernetes_label_selector"`
}

func ListService(sf ServiceFilter) (res []Service, err error) {
	if len(sf.NameInclude) == 0 {
		return nil, fmt.Errorf("param error")
	}
	if len(sf.Protocol) == 0 {
		sf.Protocol = "TCP"
	}
	if sf.Port == 0 {
		sf.Port = 80
	}
	nameFilter, err := filter.Compile(sf.NameInclude)
	if err != nil {
		return nil, fmt.Errorf("failed to compile")
	}
	cli, to := GetClient()

	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	ls, err := labels.Parse(sf.KubernetesLabelSelector)
	if err != nil {
		return nil, err
	}
	services, err := cli.CoreV1().Services(sf.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: ls.String(),
	})
	if err != nil {
		return nil, err
	}

	for _, item := range services.Items {
		if !nameFilter.Match(item.ObjectMeta.Name) {
			continue
		}
		clusterIP := item.Spec.ClusterIP
		if len(clusterIP) == 0 {
			continue
		}
		for _, p := range item.Spec.Ports {
			if string(p.Protocol) == sf.Protocol && p.Port == sf.Port {
				res = append(res, Service{
					Port:      p.Port,
					Name:      item.ObjectMeta.Name,
					Namespace: item.ObjectMeta.Namespace,
					Address:   clusterIP,
					Protocol:  string(p.Protocol),
				})
			}
		}
	}
	return res, nil
}
