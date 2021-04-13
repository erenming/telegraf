package hostsummary

import (
	"os"
	"path/filepath"

	"github.com/influxdata/telegraf"
	"github.com/shirou/gopsutil/disk"
)

// DiskCollector .
type DiskCollector struct {
	hostMountPrefix string
}

// Gather .
func (c *DiskCollector) Gather(tags map[string]string, fields map[string]interface{}, acc telegraf.Accumulator) error {
	hostMountPrefix := os.Getenv("HOST_MOUNT_PREFIX")
	mp := "/"
	if len(hostMountPrefix) > 0 {
		mp = filepath.Join(hostMountPrefix, mp)
	}
	du, err := disk.Usage(mp)
	if err != nil {
		return err
	}
	fields["disk_total"] = du.Total
	fields["disk_free"] = du.Free
	fields["disk_used"] = du.Used
	if du.Total > 0 {
		fields["disk_used_percent"] = float64(du.Used) * 100 / float64(du.Total)
	} else {
		fields["disk_used_percent"] = 0
	}
	fields["disk_fstype"] = du.Fstype
	return nil
}

func init() {
	RegisterCollector("disk", func(map[string]interface{}) Collector {
		return &DiskCollector{}
	})
}
