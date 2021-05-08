package netpackets

import (
	"log"
	"net"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/global/node"
)

// NetPackets .
type NetPackets struct {
	started bool
	HostIP  string `toml:"host_ip"`
	Enable  bool   `toml:"enable"`
}

// Description .
func (_ *NetPackets) Description() string { return "" }

// SampleConfig .
func (_ *NetPackets) SampleConfig() string { return `` }

// Gather .
func (n *NetPackets) Gather(acc telegraf.Accumulator) error {
	if !n.Enable {
		return nil
	}
	if !n.started {
		err := n.startGatter(acc)
		if err != nil {
			return err
		}
		n.started = true
	}
	return nil
}

const (
	snapshotLen = 2048
	timeout     = 30 * time.Second
	externalIP  = "external"
	localIP     = "local"
)

type packetCounter struct {
	Bytes      int64
	Packets    int64
	TCPBytes   int64
	UDPBytes   int64
	TCPPackets int64
	UDPPackets int64
}

func (n *NetPackets) startGatter(acc telegraf.Accumulator) error {
	if len(n.HostIP) <= 0 {
		info := node.GetInfo()
		n.HostIP = info.HostIP()
	}
	log.Println("host ip: ", n.HostIP)
	inter, err := getNetInterface(n.HostIP)
	if err != nil {
		return err
	}
	handle, err := pcap.OpenLive(inter.Device, snapshotLen, false, timeout)
	if err != nil {
		return err
	}
	getIPString := func(ip net.IP) string {
		if netip := ip.Mask(inter.NetMask); netip.Equal(inter.NetIP) {
			return ip.String()
		} else if netip := ip.Mask(inter.LocalMask); netip.Equal(inter.LocalIP) {
			return localIP
		}
		return externalIP
	}
	go func(acc telegraf.Accumulator) {
		defer func() {
			handle.Close()
			log.Printf("exist recv packets")
			n.started = false
		}()
		packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
		last := make(map[string]map[string]*packetCounter)
		tick := time.Tick(30 * time.Second)
		for {
			select {
			case packet, ok := <-packetSource.Packets():
				if !ok {
					return
				}
				layer := packet.Layer(layers.LayerTypeIPv4)
				if layer != nil {
					ip, _ := layer.(*layers.IPv4)
					src, dst := getIPString(ip.SrcIP), getIPString(ip.DstIP)
					m, ok := last[src]
					if !ok {
						m = make(map[string]*packetCounter)
						last[src] = m
					}
					data, ok := m[dst]
					if !ok {
						data = &packetCounter{}
						m[dst] = data
					}
					length := int64(len(packet.Data()))
					data.Bytes += length
					data.Packets++
					if ip.Protocol == layers.IPProtocolTCP {
						data.TCPBytes += length
						data.TCPPackets++
					} else if ip.Protocol == layers.IPProtocolUDP || ip.Protocol == layers.IPProtocolUDPLite {
						data.UDPBytes += length
						data.UDPPackets++
					}
				}
			case <-tick:
				for src, m := range last {
					for dst, data := range m {
						acc.AddFields("net_packets", map[string]interface{}{
							"bytes":       data.Bytes,
							"packets":     data.Packets,
							"tcp_bytes":   data.TCPBytes,
							"tcp_packets": data.TCPPackets,
							"udp_bytes":   data.UDPBytes,
							"udp_packets": data.UDPPackets,
						}, map[string]string{
							"src": src,
							"dst": dst,
						})
					}
				}
				last = make(map[string]map[string]*packetCounter)
			}
		}
	}(acc)
	return nil
}

// Interface .
type Interface struct {
	Device    string
	LocalIP   net.IP
	LocalMask net.IPMask
	NetIP     net.IP
	NetMask   net.IPMask
}

func getNetInterface(host string) (*Interface, error) {
	is, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var inter Interface
	for _, ifi := range is {
		if ifi.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err == nil {
			for _, addr := range addrs {
				if ipAddr, ok := addr.(*net.IPNet); ok {
					if ifi.Flags&net.FlagLoopback != 0 {
						if net.IPv4len == len(ipAddr.Mask) {
							inter.LocalIP = ipAddr.IP.Mask(ipAddr.Mask)
							inter.LocalMask = ipAddr.Mask
						}
					} else if ipAddr.IP.String() == host {
						inter.Device = ifi.Name
						inter.NetIP = ipAddr.IP.Mask(ipAddr.Mask)
						inter.NetMask = ipAddr.Mask
					}
				}
			}
		}
	}
	return &inter, nil
}

func init() {
	inputs.Add("net_packets", func() telegraf.Input {
		return &NetPackets{}
	})
}
