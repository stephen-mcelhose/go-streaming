package examples_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

// NetworkPacket represents a captured packet from a network interface (e.g. AF_PACKET / eBPF).
type NetworkPacket struct {
	SrcIP    string
	DstPort  int
	Protocol string
	Bytes    int
}

// SecurityAlert represents a synthesized alert for the incident response team.
type SecurityAlert struct {
	Level   string
	Message string
}

// Example_networkEngineer demonstrates how a Network / SecOps Engineer
// builds a high-throughput telemetry demuxing and alert squashing pipeline:
// 1. Ingests raw network packet captures from network interfaces.
// 2. Partitions packets by protocol (TCP vs UDP) into dedicated processing streams via routing.Partition.
// 3. Replicates security telemetry to multiple sinks (SIEM archive and real-time IDS) via routing.BroadcastBestEffort.
// 4. Rate-limits (control.Throttle) alert storms to prevent pager flooding during active attacks.
func Example_networkEngineer() {
	ctx := context.Background()

	// Ingress raw packet stream
	packetCh := networkPacketStream()

	// Step 1: Partition packets by transport protocol
	tcpSink := make(chan NetworkPacket, 5)
	udpSink := make(chan NetworkPacket, 5)

	routes := map[string]streaming.Sender[NetworkPacket]{
		"TCP": streaming.NewChanStream(nil, tcpSink),
		"UDP": streaming.NewChanStream(nil, udpSink),
	}

	_ = routing.Partition(ctx,
		func(p NetworkPacket) (string, error) { return p.Protocol, nil },
		routes,
		nil,
		streaming.NewChanStream(packetCh, nil),
	)

	close(tcpSink)
	close(udpSink)

	fmt.Printf("Demuxed: %d TCP packets, %d UDP packets\n", len(tcpSink), len(udpSink))

	// Step 2: Multi-destination fan-out via BroadcastBestEffort (Non-blocking SIEM & IDS telemetry)
	alertCh := securityAlertStream()
	siemSink := make(chan SecurityAlert, 1)
	idsSink := make(chan SecurityAlert, 1)

	_ = routing.BroadcastBestEffort(ctx,
		streaming.NewChanStream(alertCh, nil),
		streaming.NewChanStream(nil, siemSink),
		streaming.NewChanStream(nil, idsSink),
	)

	close(siemSink)
	close(idsSink)
	fmt.Printf("Telemetry replicated to SIEM (%d) and IDS (%d)\n", len(siemSink), len(idsSink))

	// Step 3: Throttle burst alerts to prevent on-call pager flood
	burstAlerts := ddosAlertBurstStream()
	pagerSink := make(chan SecurityAlert, 2)
	go func() {
		defer close(pagerSink)
		_ = control.Throttle(ctx, 10*time.Millisecond,
			streaming.NewChanStream(burstAlerts, nil),
			streaming.NewChanStream(nil, pagerSink),
		)
	}()

	for alert := range pagerSink {
		fmt.Printf("Pager Alert: [%s] %s\n", alert.Level, alert.Message)
	}

	// Output:
	// Demuxed: 3 TCP packets, 1 UDP packets
	// Telemetry replicated to SIEM (1) and IDS (1)
	// Pager Alert: [CRITICAL] SYN Flood #1
	// Pager Alert: [CRITICAL] SYN Flood #2
}

// networkPacketStream simulates real-time packet ingress captured from network interfaces via eBPF/PCAP.
// For Network Engineers and NetOps teams, this feed contains mixed transport protocols, including standard
// HTTP web traffic (TCP:80), DNS lookups (UDP:53), and port probes targeting SMB (TCP:445) and RDP (TCP:3389).
func networkPacketStream() <-chan NetworkPacket {
	packetCh := make(chan NetworkPacket, 4)
	packetCh <- NetworkPacket{SrcIP: "192.168.1.50", DstPort: 80, Protocol: "TCP", Bytes: 120}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 445, Protocol: "TCP", Bytes: 64} // SMB Probe
	packetCh <- NetworkPacket{SrcIP: "192.168.1.51", DstPort: 53, Protocol: "UDP", Bytes: 512}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 3389, Protocol: "TCP", Bytes: 64} // RDP Probe
	close(packetCh)
	return packetCh
}

// securityAlertStream produces synthesized high-severity incident notifications from an IDS detector.
// For SecOps and Observability engineers, this stream must be replicated simultaneously across multiple
// destinations (e.g. archival cold storage and real-time SIEM indexers) using non-blocking best-effort semantics.
func securityAlertStream() <-chan SecurityAlert {
	alertCh := make(chan SecurityAlert, 1)
	alertCh <- SecurityAlert{Level: "CRITICAL", Message: "Port Scan Detected from 10.0.0.99"}
	close(alertCh)
	return alertCh
}

// ddosAlertBurstStream models a sudden storm of high-frequency incident triggers emitted during an active attack.
// In network operations and on-call alerting systems, unthrottled burst streams exhaust notification API rate limits
// and cause responder fatigue; Throttle enforces a deterministic maximum dispatch rate across the stream.
func ddosAlertBurstStream() <-chan SecurityAlert {
	burstAlerts := make(chan SecurityAlert, 2)
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #1"}
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #2"}
	close(burstAlerts)
	return burstAlerts
}
