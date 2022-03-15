package addons

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/toml"
	"github.com/influxdata/toml/ast"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
)

func (a *Addons) checkAndAddInputV2(pod *apiv1.Pod, acc telegraf.Accumulator) (string, error) {
	envs, err := a.extractInfo(pod)
	if err != nil {
		return "", err
	}
	if envs == nil {
		return "", nil
	}
	// 以前一个addon一个telegraf 时，telegraf里也会有ADDON_TYPE，避免匹配这些容器
	if envs["DICE_COMPONENT"] == "spot/telegraf" {
		return "", nil
	}
	key, typ := envs[a.InputKeyName], envs[a.TemplateKeyName]
	if key == "" || typ == "" {
		return "", nil
	}
	a.inputsLock.Lock()
	defer a.inputsLock.Unlock()

	info, ok := a.inputs[key]
	if ok {
		if reflect.DeepEqual(info.envs, envs) {
			return key, nil
		} else {
			log.Printf("checkAndAddInput, delete input, info:%+v, env:%+v", info.envs, envs)
			a.deleteInput(key, info)
		}
	}

	t, ok := a.Templates[typ]
	if !ok {
		return "", nil
	}
	for k, v := range t.EnvMatch {
		if v != "*" {
			if envs[k] != v {
				// not match
				return "", nil
			}
		} else {
			if _, ok := envs[k]; !ok {
				// not match
				return "", nil
			}
		}
	}
	envTags := make(map[string]string)
	for k, v := range envs {
		if a.tagEnvFilter.Match(k) {
			envTags[strings.ToLower(k)] = v
		}
	}
	acc = accumulatorWithTags{acc, envTags}
	info = &inputInfo{
		envs: envs,
		tags: envTags,
		name: typ,
	}
	a.inputs[key] = info
	buf := &bytes.Buffer{}
	err = t.cfgTmp.Execute(buf, envs)
	if err != nil {
		log.Printf("render template failed, error:%v", err)
		return "", err
	}
	tbl, err := toml.Parse(buf.Bytes())
	if err != nil {
		log.Printf("parse envs failed, envs:%+v", envs)
		return "", err
	}
	for inputName, val := range tbl.Fields {
		switch subTable := val.(type) {
		case *ast.Table:
			if err = a.addInput(inputName, info, subTable, acc); err != nil {
				return "", err
			}
		case []*ast.Table:
			for _, t := range subTable {
				if err = a.addInput(inputName, info, t, acc); err != nil {
					return "", err
				}
			}
		default:
			return "", fmt.Errorf("%s: invalid configuration", inputName)
		}
	}
	log.Printf("I! [addon] add input %s, key=%s ok", typ, key)
	return key, nil
}

func (a *Addons) checkAndStopV2(pod *apiv1.Pod) error {
	envs, err := a.extractInfo(pod)
	if err != nil {
		return err
	}
	if envs != nil {
		return nil
	}
	key := envs[a.InputKeyName]
	a.inputsLock.Lock()
	if info, ok := a.inputs[key]; ok {
		log.Printf("checkAndStop, key:%s, input:%+v", key, a.inputs)
		a.deleteInput(key, info)
	}
	a.inputsLock.Unlock()
	return nil
}

func (a *Addons) extractInfo(pod *apiv1.Pod) (map[string]string, error) {
	envs := map[string]string{
		"SELF_HOST": pod.Status.PodIP,
	}
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("pod<%s/%s> must have containers", pod.Namespace, pod.Name)
	}
	firstC := pod.Spec.Containers[0]
	for _, v := range firstC.Env {
		if v.ValueFrom != nil {
			continue
		}
		envs[v.Name] = v.Value
	}
	envs = a.handleEnvMap(pod, envs)

	// key, ok := envs[a.InputKeyName]
	// if !ok {
	// 	return nil, nil
	// }
	typ, ok := envs[a.TemplateKeyName]
	if !ok {
		return nil, nil
	}
	if typ == "redis" {
		// 暂时不监控 sentinel、避免redis和sentinel在同一台节点上时，addon_id一样导致冲突
		if envs["REDIS_ROLE"] == "sentinel" {
			return nil, nil
		}
		if strings.Contains(envs["SELF_HOST"], "sentinel") {
			return nil, nil
		}
	}
	return envs, nil
	// return envs, typ, envs["ADDON_ID"], fmt.Sprintf("%s-%s", typ, key), err
}

func (a *Addons) handleEnvMap(pod *apiv1.Pod, envs map[string]string) map[string]string {
	podName := pod.Name
	idx := strings.LastIndex(podName, "-")
	if idx == -1 {
		return envs
	}
	_, err := strconv.Atoi(podName[idx+1:])
	if err != nil {
		return envs
	}

	prefix := "N" + podName[idx+1:] + "_"
	for k, v := range envs {
		if strings.HasPrefix(k, prefix) {
			envs[k[len(prefix):]] = v
		}
	}
	id, ok := envs["ADDON_ID"]
	if !ok {
		return envs
	}

	typ, ok := envs["ADDON_TYPE"]
	if !ok {
		return envs
	}
	gtype := typ
	if typ == "terminus-elasticsearch" {
		gtype = "elasticsearch"
		if _, ok := envs["ES_PASSWORD"]; !ok {
			envs["ES_PASSWORD"] = ""
		}
	}

	if _, ok := a.Templates[typ]; !ok {
		return envs
	}

	switch gtype {
	case "elasticsearch":
		if port, ok := envs["TERMINUS_ELASTICSEARCH_SERVICE_PORT"]; ok {
			envs["SELF_PORT"] = port
		} else {
			envs["SELF_PORT"] = "9200"
		}
		if _, ok := envs["ES_PASSWORD"]; !ok {
			envs["ES_PASSWORD"] = ""
		}
	case "mysql":
		envs["SELF_PORT"] = "3306"
	case "redis":
		if port, ok := envs["REDIS_PORT"]; ok {
			envs["SELF_PORT"] = port
		} else {
			envs["SELF_PORT"] = "6379"
		}
	}
	if envs["SELF_HOST"] == "" { // use FQDN
		url := fmt.Sprintf("%s.%s.group-addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
		if _, err := net.LookupHost(url); err == nil {
			envs["SELF_HOST"] = url
		} else {
			url = fmt.Sprintf("%s.%s.addon-%s--%s.svc.cluster.local", podName, typ, gtype, id)
			if _, err := net.LookupHost(url); err == nil {
				envs["SELF_HOST"] = url
			}
		}
	}
	return envs

}

func (a *Addons) loadAddonsFromK8s(ctx context.Context, acc telegraf.Accumulator, client *k8s.Clientset) error {
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: a.LabelSelector,
		FieldSelector: a.FieldSelector,
	})
	if err != nil {
		return fmt.Errorf("get pod list: %s", err)
	}
	all := make(map[string]struct{})
	for _, pod := range pods.Items {
		key, err := a.checkAndAddInputV2(&pod, acc)
		if err != nil {
			log.Printf("E! [addon] loadAddons.checkAndAddInput(%s/%s) : %v", pod.Namespace, pod.Name, err)
			continue
		}
		if key != "" {
			all[key] = struct{}{}
		}
	}
	a.inputsLock.Lock()
	for key, info := range a.inputs {
		if _, ok := all[key]; !ok {
			log.Printf("load addons, delete input, key:%s, inputs:%+v, all:%+v", key, a.inputs, all)
			a.deleteInput(key, info)
		}
	}
	a.inputsLock.Unlock()
	return nil
}
