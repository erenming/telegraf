package dockersummary

import (
	"github.com/influxdata/telegraf/plugins/inputs/global/kubelet"
)

func (s *Summary) gatherContainerNet(gtx *gatherContext) {
	if !gtx.info.State.Running {
		return
	}
	if s.k8s {
		s.getContainerNetStats(gtx)
		return
	}
	var (
		rxBytes, txBytes     uint64
		rxPackets, txPackets uint64
		rxErrors, txErrors   uint64
		rxDropped, txDropped uint64
	)
	for _, netstats := range gtx.stats.Networks {
		rxBytes += netstats.RxBytes
		txBytes += netstats.TxBytes

		rxPackets += netstats.RxPackets
		txPackets += netstats.TxPackets

		rxErrors += netstats.RxErrors
		txErrors += netstats.TxErrors

		rxDropped += netstats.RxDropped
		txDropped += netstats.TxDropped
	}

	gtx.fields["rx_bytes"] = rxBytes
	gtx.fields["tx_bytes"] = txBytes
	gtx.fields["rx_packets"] = rxPackets
	gtx.fields["tx_packets"] = txPackets
	gtx.fields["rx_errors"] = rxErrors
	gtx.fields["tx_errors"] = txErrors
	gtx.fields["rx_dropped"] = rxDropped
	gtx.fields["tx_dropped"] = txDropped
}

func (s *Summary) getContainerNetStats(gtx *gatherContext) {
	if s.podNetStatus == nil || gtx.info == nil || gtx.info.Config == nil {
		return
	}
	metadata := kubelet.PodID{
		Name:      gtx.info.Config.Labels["io.kubernetes.pod.name"],
		Namespace: gtx.info.Config.Labels["io.kubernetes.pod.namespace"],
	}
	if stat, ok := s.podNetStatus[metadata]; ok {
		gtx.fields["rx_bytes"] = stat.Network.RxBytes
		gtx.fields["tx_bytes"] = stat.Network.TxBytes

		gtx.fields["rx_errors"] = stat.Network.RxErrors
		gtx.fields["tx_errors"] = stat.Network.TxErrors
	}
}
