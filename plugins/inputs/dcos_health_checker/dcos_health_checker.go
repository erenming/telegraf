package dcos_health_checker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/common/util"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type DCOSHealthChecker struct {
	ClusterPath string `toml:"cluster_path"`
	Masters     []string

	MasterHealthPort      string `toml:"master_health_port"`
	SlaveHealthPort       string `toml:"slave_health_port"`
	UnitsHealthPort       string `toml:"units_health_port"`
	GatherNodeHealth      bool   `toml:"gather_node_health"`
	GatherNodeUnitsHealth bool   `toml:"gather_node_units_health"`

	Timeout config.Duration
	tls.ClientConfig
	initialized bool
	client      *http.Client

	masters         []*nodeStatus
	masterIndex     int
	myStatus        *nodeStatus
	getMyStatusTime time.Time
}

type nodeStatus struct {
	host   string
	health bool
	time   time.Time
}

var sampleConfig = `
[[inputs.dcos_health_checker]]
  masters = ["10.168.0.80"]
  cluster_path = "/etc/cluster-node"
  master_health_port = "5050"
  slave_health_port = "5051"
  units_health_port = "80"
  gather_node_health = true
  gather_node_units_health = true

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Amount of time allowed to complete the Gather request
  # timeout = "5s"
`

// SampleConfig returns the default configuration of the Input
func (*DCOSHealthChecker) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description on the Input
func (*DCOSHealthChecker) Description() string {
	return "Gather metric about dcos health"
}

// Gather takes in an accumulator and adds the metrics that the Input
// gathers. This is called every "interval"
func (d *DCOSHealthChecker) Gather(acc telegraf.Accumulator) error {
	if d.initialized == false {
		clusterPath := os.Getenv("CLUSTER_PATH")
		if clusterPath != "" {
			d.ClusterPath = clusterPath
		}
		d.masters = make([]*nodeStatus, 0, len(d.Masters))
		now := time.Now()
		for _, m := range d.Masters {
			if len(m) == 0 {
				continue
			}
			d.masters = append(d.masters, &nodeStatus{
				host:   m,
				health: true,
				time:   now,
			})
		}
		tlsCfg, err := d.ClientConfig.TLSConfig()
		if err != nil {
			return err
		}
		d.client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsCfg,
				Proxy:           http.ProxyFromEnvironment,
			},
			Timeout: time.Duration(d.Timeout),
		}

		d.initialized = true
	}
	node, err := util.GetClusterNodeInfo(d.ClusterPath)
	if err != nil {
		return err
	}
	now := time.Now()
	if node.NodeType == "master" && (d.myStatus == nil || d.getMyStatusTime.Add(120*time.Second).Before(now)) {
		for _, m := range d.masters {
			if m.host == node.HostIP {
				d.myStatus = m
				break
			}
		}
		d.getMyStatusTime = now
	}

	var wg sync.WaitGroup
	if d.GatherNodeHealth {
		var url string
		if node.NodeType == "master" {
			url = fmt.Sprintf("http://%s:%s/health", node.HostIP, d.MasterHealthPort)
		} else {
			url = fmt.Sprintf("http://%s:%s/health", node.HostIP, d.SlaveHealthPort)
		}
		wg.Add(1)
		go func(s, url string) {
			defer wg.Done()
			if err := d.gatherHealth(acc, url, s, now); err != nil {
				acc.AddError(fmt.Errorf("[url=%s]: %s", url, err))
			}
		}(node.HostIP, url)
	}
	if d.GatherNodeUnitsHealth && len(d.masters) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				master := d.getMasterStatus(node)
				if master == nil {
					acc.AddError(fmt.Errorf("dcos master not available"))
					return
				}
				url := fmt.Sprintf("http://%s:%s/system/health/v1/nodes/%s/units", master.host, d.UnitsHealthPort, node.HostIP)
				if err := d.gatherNodeUnitsHealth(acc, url, master.host, node.HostIP, now); err != nil {
					master.setStatus(false)
					acc.AddError(fmt.Errorf("[url=%s]: %s", url, err))
				} else {
					return
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func (d *DCOSHealthChecker) gatherHealth(
	acc telegraf.Accumulator,
	url, server string,
	now time.Time,
) error {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	fields := make(map[string]interface{})
	tags := map[string]string{"node": server}
	resp, err := d.client.Do(request)
	if err != nil {
		fields["health"] = 1
		tags["health"] = "false"
		tags["reason"] = "request"
		acc.AddFields("dcos_node_health", fields, tags, now)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fields["health"] = 1
		tags["health"] = "false"
		tags["reason"] = "status"
		acc.AddFields("dcos_node_health", fields, tags, now)
		return nil
	}
	fields["health"] = 0
	tags["health"] = "true"
	acc.AddFields("dcos_node_health", fields, tags, now)
	return nil
}

func (d *DCOSHealthChecker) gatherNodeUnitsHealth(
	acc telegraf.Accumulator,
	url, master, node string,
	now time.Time,
) error {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := d.client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Received status code %d (%s), expected %d (%s)",
			resp.StatusCode,
			http.StatusText(resp.StatusCode),
			http.StatusOK,
			http.StatusText(http.StatusOK))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	units := &NodeUnits{}
	err = json.Unmarshal(b, units)
	if err != nil {
		return err
	}
	fields := make(map[string]interface{})
	tags := map[string]string{"node": node, "master": master}
	for _, u := range units.Units {
		key := strings.Replace(u.ID, ".", "_", -1)
		key = strings.Replace(key, "-", "_", -1)
		fields[key] = u.Health
		if u.Health != 0 {
			tags["error"] = "true"
		} else {
			tags["error"] = "false"
		}
	}
	acc.AddFields("dcos_node_units_health", fields, tags, now)
	return nil
}

func (d *DCOSHealthChecker) getMasterStatus(node *util.ClusterNode) *nodeStatus {
	now := time.Now()
	if d.myStatus != nil && node.NodeType == "master" && d.myStatus.health {
		return d.myStatus
	} else {
		for i, length := 0, len(d.masters); i < length; i++ {
			ns := d.masters[d.masterIndex]
			d.masterIndex = (d.masterIndex + 1) % length
			if ns.health == true {
				return ns
			} else {
				if ns.time.Add(60 * time.Second).Before(now) {
					ns.setStatus(true)
					return ns
				}
			}
		}
	}
	return nil
}

func (ns *nodeStatus) setStatus(health bool) {
	if ns.health != health {
		ns.time = time.Now()
	}
	ns.health = health
}

type NodeUnits struct {
	Units []*NodeUnit `json:"units"`
}

type NodeUnit struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Health      int    `json:"health"`
}

func init() {
	inputs.Add("dcos_health_checker", func() telegraf.Input {
		return &DCOSHealthChecker{
			Timeout:               config.Duration(time.Second * 5),
			GatherNodeHealth:      true,
			GatherNodeUnitsHealth: true,
		}
	})
}
