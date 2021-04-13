package dockersummary

import (
	"github.com/docker/docker/api/types"
	"github.com/influxdata/telegraf/plugins/inputs/global/kubelet"
)

func (s *Summary) gatherContainerNet(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string, info *types.ContainerJSON,
	daemonOSType string, stats *types.StatsJSON) {
	if !info.State.Running {
		return
	}
	if s.k8s {
		s.getContainerNetStats(id, tags, fields, envs, info, daemonOSType, stats)
		return
	}
	var (
		rxBytes, txBytes     uint64
		rxPackets, txPackets uint64
		rxErrors, txErrors   uint64
		rxDropped, txDropped uint64
	)
	for _, netstats := range stats.Networks {
		rxBytes += netstats.RxBytes
		txBytes += netstats.TxBytes

		rxPackets += netstats.RxPackets
		txPackets += netstats.TxPackets

		rxErrors += netstats.RxErrors
		txErrors += netstats.TxErrors

		rxDropped += netstats.RxDropped
		txDropped += netstats.TxDropped
	}

	fields["rx_bytes"] = rxBytes
	fields["tx_bytes"] = txBytes

	fields["rx_packets"] = rxPackets
	fields["tx_packets"] = txPackets

	fields["rx_errors"] = rxErrors
	fields["tx_errors"] = txErrors

	fields["rx_dropped"] = rxDropped
	fields["tx_dropped"] = txDropped
}

func (s *Summary) getContainerNetStats(
	id string, tags map[string]string, fields map[string]interface{},
	envs map[string]string, info *types.ContainerJSON,
	daemonOSType string, stats *types.StatsJSON) {
	if s.podNetStatus == nil || info == nil || info.Config == nil {
		return
	}
	metadata := kubelet.PodID{
		Name:      info.Config.Labels["io.kubernetes.pod.name"],
		Namespace: info.Config.Labels["io.kubernetes.pod.namespace"],
	}
	if stat, ok := s.podNetStatus[metadata]; ok {
		fields["rx_bytes"] = stat.Network.RxBytes
		fields["tx_bytes"] = stat.Network.TxBytes

		fields["rx_errors"] = stat.Network.RxErrors
		fields["tx_errors"] = stat.Network.TxErrors
	}
}
