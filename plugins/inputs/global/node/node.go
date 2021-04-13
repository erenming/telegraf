package node

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global"
)

// collector .
type collector struct {
	global.Base
	ClusterPath string `toml:"cluster_path"`
	ClusterType string `toml:"cluster_type"`
	LabelsPath  string `toml:"labels_path"`

	info
	labels
	etcPath, procPath string

	lastLabelsGather time.Time
	lastInfoGather   time.Time

	lock sync.Mutex
}

// Start .
func (c *collector) Start(acc telegraf.Accumulator) error {
	c.etcPath, c.procPath = os.Getenv("HOST_ETC"), os.Getenv("HOST_PROC")
	c.LabelsPath = util.Getenv(c.LabelsPath, "LABELS_PATH")
	c.ClusterType = util.Getenv(c.ClusterType, "DICE_CLUSTER_TYPE", "CLUSTER_TYPE")
	c.ClusterPath = util.Getenv(c.ClusterPath, "CLUSTER_PATH")
	c.clusterType = c.ClusterType
	return nil
}

func (c *collector) gaterNodeInfo() error {
	now := time.Now()
	if c.lastInfoGather.Add(30 * time.Minute).After(now) {
		return nil
	}
	node, err := util.GetClusterNodeInfo(c.ClusterPath)
	if err != nil {
		return err
	}
	c.clusterName = node.ClusterName
	c.hostIP = node.HostIP
	if c.IsK8s() {
		if c.nodeName == "" {
			c.populateWithK8S()
		}
	}
	if c.nodeName == "" {
		c.nodeName = c.hostIP
		log.Printf("use node ip %s as node name", c.nodeName)
	}
	if c.nodeName == "" {
		c.nodeName = os.Getenv("K8S_NODE_NAME")
	}

	var hostname string
	if len(c.etcPath) > 0 || len(c.procPath) > 0 {
		hostname, err = util.GetHostname(c.etcPath, c.procPath) // 获取宿主机的 hostname
	} else {
		hostname, err = os.Hostname()
	}
	if err != nil {
		log.Printf("fail to get hostname: %s", err)
	} else {
		c.hostname = hostname
	}
	c.lastInfoGather = now
	return nil
}

// gatterNodeLabels .
func (c *collector) gatterNodeLabels() (err error) {
	now := time.Now()
	if c.lastLabelsGather.Add(30 * time.Second).After(now) {
		return nil
	}
	var line string
	labels := make(map[string]string)
	if c.IsK8s() {
		line, labels, err = c.getK8sNodeLabels(c.nodeName)
		if err != nil {
			//log.Printf("fail to get k8s node %s labels %s", c.nodeName, err)
			return err
		}
	} else {
		line, err = util.ReadLabelsFile(c.LabelsPath)
		if err != nil {
			//log.Printf("fail to read labels file: %s", err)
			return err
		}
		if len(line) > len("MESOS_ATTRIBUTES=dice_tags:") {
			line := line[len("MESOS_ATTRIBUTES=dice_tags:"):]
			for _, label := range strings.Split(line, ",") {
				label = strings.TrimSpace(label)
				if len(label) > 0 {
					labels[label] = "true"
				}
			}
		}
	}
	c.lablesLine = line
	c.lablesMap = labels
	for k := range labels {
		if strings.HasPrefix(k, "org-") {
			c.orgName = k[len("org-"):]
			break
		}
	}
	c.lastLabelsGather = now
	log.Printf("org: %s, node labels: %s \n", c.orgName, labels)
	return err
}

var instance = &collector{
	LabelsPath:  "/rootfs/var/lib/dcos/mesos-slave-common",
	ClusterType: "dcos",
	ClusterPath: "/etc/cluster-node", // 历史遗留
}

func init() {
	inputs.Add("node", func() telegraf.Input {
		return instance
	})
}

// Info .
type Info interface {
	NodeName() string
	Hostname() string
	HostIP() string
	ClusterType() string
	ClusterName() string
	IsK8s() bool
	AllocatableMEM() float64
	AllocatableCPU() float64
}

type info struct {
	nodeName    string
	hostname    string
	hostIP      string
	clusterType string
	clusterName string

	// for k8s
	allocatable map[string]float64
}

func (i *info) NodeName() string    { return i.nodeName }
func (i *info) Hostname() string    { return i.hostname }
func (i *info) HostIP() string      { return i.hostIP }
func (i *info) ClusterType() string { return i.clusterType }
func (i *info) ClusterName() string { return i.clusterName }
func (i *info) IsK8s() bool {
	return i.clusterType == "k8s" || i.clusterType == "kubernetes"
}
func (i *info) AllocatableCPU() float64 {
	return i.allocatable["cpu"]
}

func (i *info) AllocatableMEM() float64 {
	return i.allocatable["memory"]
}

// GetInfo .
func GetInfo() Info {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	instance.gaterNodeInfo()
	i := instance.info
	return &i
}

// Labels .
type Labels interface {
	Line() string
	Map() map[string]string
	OrgName() string
	List() []string
}

type labels struct {
	lablesLine string
	lablesMap  map[string]string
	orgName    string
}

func (l *labels) Line() string           { return l.lablesLine }
func (l *labels) Map() map[string]string { return l.lablesMap }
func (l *labels) OrgName() string        { return l.orgName }
func (l *labels) List() []string {
	var list []string
	for k := range l.lablesMap {
		list = append(list, k)
	}
	return list
}

// GetLabels .
func GetLabels() (Labels, error) {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	err := instance.gatterNodeLabels()
	if err != nil {
		return nil, err
	}
	ls := instance.labels
	return &ls, nil
}
