package util

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/influxdata/toml"
	"github.com/pkg/errors"
)

func ReadClusterNodeFile(path string) (*ClusterNode, error) {
	info := &ClusterNode{Others: make(map[string]interface{})}
	if path == "" {
		return info, errors.Errorf("cluster path is empty")
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return info, errors.Errorf("fail to read line %s: %s", path, err)
	}
	if err := toml.Unmarshal(data, &info.Others); err != nil {
		return info, errors.Errorf("fail to toml unmarshal %s: %s", path, err)
	}
	if v, ok := info.Others["cluster_name"]; ok {
		info.ClusterName, _ = v.(string)
	}
	if len(info.ClusterName) <= 0 {
		if v, ok := info.Others["cluster"]; ok {
			info.ClusterName, _ = v.(string)
		}
	}
	if v, ok := info.Others["node_ip"]; ok {
		info.HostIP, _ = v.(string)
	}
	if v, ok := info.Others["node_type"]; ok {
		info.NodeType, _ = v.(string)
	}
	// if v, ok := info.Others["node_name"]; ok {
	// 	info.NodeName, _ = v.(string)
	// }
	delete(info.Others, "cluster")
	delete(info.Others, "cluster_name")
	delete(info.Others, "node_ip")
	delete(info.Others, "node_type")
	// delete(info.Others, "node_name")

	if info.ClusterName == "" {
		return info, errors.Errorf("cluster must not be empty")
	}
	if info.HostIP == "" {
		return info, errors.Errorf("node_ip must not be empty")
	}
	if info.NodeType == "" {
		return info, errors.Errorf("node_type must not be empty")
	}
	return info, nil
}

func ReadLabelsFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", errors.Errorf("fail to read file %s: %s", path, err)
	}
	labels := strings.TrimSpace(string(data))
	if os.Getenv("DICE_CLUSTER_TYPE") == "dcos" {
		if labels == "" {
			labels = "MESOS_ATTRIBUTES=dice_tags:"
		}
		if !strings.Contains(labels, "org-") {
			orgName := os.Getenv("DICE_ORG")
			if orgName != "" {
				if strings.HasSuffix(labels, "dice_tags:") {
					labels += "org-" + orgName
				} else {
					labels += ",org-" + orgName
				}
			}
		}
	}
	return labels, nil
}

func GetHostname(etcPath, procPath string) (string, error) {
	if etcPath == "" {
		etcPath = "/etc"
	}
	if procPath == "" {
		procPath = "/proc"
	}

	etcFile := etcPath + "/hostname"
	f, err := os.Open(etcFile)
	if err == nil {
		return readHostname(f)
	}
	if !os.IsNotExist(err) {
		return "", errors.Errorf("fail to open %s: %s", etcFile, err)
	}

	procFile := procPath + "/sys/kernel/hostname"
	f, err = os.Open(procFile)
	if err != nil {
		return "", errors.Errorf("fail to open %s: %s", procFile, err)
	}
	return readHostname(f)
}

func readHostname(f *os.File) (string, error) {
	var buf [512]byte // Enough for a DNS name.
	n, err := f.Read(buf[0:])
	if err != nil {
		return "", errors.Errorf("fail to read file, %s", err)
	}

	if n > 0 && buf[n-1] == '\n' {
		n--
	}
	return string(buf[0:n]), nil
}

// Getenv .
func Getenv(def string, keys ...string) string {
	for _, key := range keys {
		val := os.Getenv(key)
		if len(val) > 0 {
			return val
		}
	}
	return def
}


func Ns2Ms(nano int64) int64 {
	return nano / 1000000
}

func StringIn(s string, in []string) bool {
	existed := false
	for _, item := range in {
		if item == s {
			existed = true
			break
		}
	}
	return existed
}
