package system

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/influxdata/telegraf/internal"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

type PS interface {
	CPUTimes(perCPU, totalCPU bool) ([]cpu.TimesStat, error)
	DiskUsage(mountPointFilter []string, fstypeExclude []string) ([]*disk.UsageStat, []*disk.PartitionStat, error)
	NetIO() ([]net.IOCountersStat, error)
	NetProto() ([]net.ProtoCountersStat, error)
	DiskIO(names []string) (map[string]disk.IOCountersStat, error)
	VMStat() (*mem.VirtualMemoryStat, error)
	SwapStat() (*mem.SwapMemoryStat, error)
	NetConnections() ([]net.ConnectionStat, error)
	Temperature() ([]host.TemperatureStat, error)
}

type PSDiskDeps interface {
	Partitions(all bool) ([]disk.PartitionStat, error)
	OSGetenv(key string) string
	OSStat(name string) (os.FileInfo, error)
	PSDiskUsage(path string) (*disk.UsageStat, error)
}

func NewSystemPS() *SystemPS {
	return &SystemPS{&SystemPSDisk{}}
}

type SystemPS struct {
	PSDiskDeps
}

type SystemPSDisk struct{}

func (s *SystemPS) CPUTimes(perCPU, totalCPU bool) ([]cpu.TimesStat, error) {
	var cpuTimes []cpu.TimesStat
	if perCPU {
		if perCPUTimes, err := cpu.Times(true); err == nil {
			cpuTimes = append(cpuTimes, perCPUTimes...)
		} else {
			return nil, err
		}
	}
	if totalCPU {
		if totalCPUTimes, err := cpu.Times(false); err == nil {
			cpuTimes = append(cpuTimes, totalCPUTimes...)
		} else {
			return nil, err
		}
	}
	return cpuTimes, nil
}

func (s *SystemPS) DiskUsage(
	mountPointFilter []string,
	fstypeExclude []string,
) ([]*disk.UsageStat, []*disk.PartitionStat, error) {
	parts, err := s.Partitions(true)
	if err != nil {
		return nil, nil, err
	}

	// Make a "set" out of the filter slice
	mountPointFilterSet := make(map[string]bool)
	for _, filter := range mountPointFilter {
		mountPointFilterSet[filter] = true
	}
	fstypeExcludeSet := make(map[string]bool)
	for _, filter := range fstypeExclude {
		fstypeExcludeSet[filter] = true
	}

	// Autofs mounts indicate a potential mount, the partition will also be
	// listed with the actual filesystem when mounted.  Ignore the autofs
	// partition to avoid triggering a mount.
	fstypeExcludeSet["autofs"] = true

	var usage []*disk.UsageStat
	var partitions []*disk.PartitionStat
	hostMountPrefix := s.OSGetenv("HOST_MOUNT_PREFIX")
	device := deviceMap(parts)

	for i := range parts {
		p := parts[i]

		if len(mountPointFilter) > 0 {
			// If the mount point is not a member of the filter set,
			// don't gather info on it.
			if _, ok := mountPointFilterSet[p.Mountpoint]; !ok {
				continue
			}
		}

		// If the mount point is a member of the exclude set,
		// don't gather info on it.
		if _, ok := fstypeExcludeSet[p.Fstype]; ok {
			continue
		}

		// exclude sub mount point which has same device
		if paths, ok := device[p.Device]; ok {
			if _, ok := paths[p.Mountpoint]; !ok {
				continue
			}
		}

		// If there's a host mount prefix, exclude any mount point which conflict
		// with the prefix.
		if len(hostMountPrefix) > 0 && !strings.HasPrefix(p.Mountpoint, hostMountPrefix) {
			continue
		}

		du, err := s.PSDiskUsage(p.Mountpoint)
		if err != nil {
			continue
		}

		du.Path = filepath.Join("/", strings.TrimPrefix(p.Mountpoint, hostMountPrefix))
		du.Fstype = p.Fstype
		usage = append(usage, du)
		partitions = append(partitions, &p)
	}

	return usage, partitions, nil
}

// one device mapped with multi mountPoint (different prefix)
func deviceMap(parts []disk.PartitionStat) map[string]map[string]struct{} {
	device := make(map[string]map[string]struct{})
	for i := range parts {
		p := parts[i]

		var ps map[string]struct{}
		if _ps, ok := device[p.Device]; ok {
			ps = _ps
		} else {
			ps = make(map[string]struct{}, 2)
			ps[p.Mountpoint] = struct{}{}
			device[p.Device] = ps
			continue
		}

		for path := range ps {
			// mount point sub of path
			target, err := filepath.Rel(path, p.Mountpoint)
			if err == nil && !strings.HasPrefix(target, ".") {
				continue
			}

			// path sub of mount point
			target, err = filepath.Rel(p.Mountpoint, path)
			if err == nil && !strings.HasPrefix(target, ".") {
				delete(ps, path)
			}

			ps[p.Mountpoint] = struct{}{}
		}
	}
	return device
}

func (s *SystemPS) NetProto() ([]net.ProtoCountersStat, error) {
	return net.ProtoCounters(nil)
}

func (s *SystemPS) NetIO() ([]net.IOCountersStat, error) {
	return net.IOCounters(true)
}

func (s *SystemPS) NetConnections() ([]net.ConnectionStat, error) {
	return net.Connections("all")
}

func (s *SystemPS) DiskIO(names []string) (map[string]disk.IOCountersStat, error) {
	m, err := disk.IOCounters(names...)
	if err == internal.ErrorNotImplemented {
		return nil, nil
	}

	return m, err
}

func (s *SystemPS) VMStat() (*mem.VirtualMemoryStat, error) {
	return mem.VirtualMemory()
}

func (s *SystemPS) SwapStat() (*mem.SwapMemoryStat, error) {
	return mem.SwapMemory()
}

func (s *SystemPS) Temperature() ([]host.TemperatureStat, error) {
	temp, err := host.SensorsTemperatures()
	if err != nil {
		_, ok := err.(*host.Warnings)
		if !ok {
			return temp, err
		}
	}
	return temp, nil
}

func (s *SystemPSDisk) Partitions(all bool) ([]disk.PartitionStat, error) {
	return disk.Partitions(all)
}

func (s *SystemPSDisk) OSGetenv(key string) string {
	return os.Getenv(key)
}

func (s *SystemPSDisk) OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (s *SystemPSDisk) PSDiskUsage(path string) (*disk.UsageStat, error) {
	return disk.Usage(path)
}
