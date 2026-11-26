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

// networkPacketStream returns a live simulated feed of raw packet captures containing TCP and UDP traffic.
func networkPacketStream() <-chan NetworkPacket {
	packetCh := make(chan NetworkPacket, 4)
	packetCh <- NetworkPacket{SrcIP: "192.168.1.50", DstPort: 80, Protocol: "TCP", Bytes: 120}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 445, Protocol: "TCP", Bytes: 64} // SMB Probe
	packetCh <- NetworkPacket{SrcIP: "192.168.1.51", DstPort: 53, Protocol: "UDP", Bytes: 512}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 3389, Protocol: "TCP", Bytes: 64} // RDP Probe
	close(packetCh)
	return packetCh
}

// securityAlertStream returns high-priority threat detection events to be replicated across log systems.
func securityAlertStream() <-chan SecurityAlert {
	alertCh := make(chan SecurityAlert, 1)
	alertCh <- SecurityAlert{Level: "CRITICAL", Message: "Port Scan Detected from 10.0.0.99"}
	close(alertCh)
	return alertCh
}

// ddosAlertBurstStream simulates a sudden burst of high-frequency attack alerts triggering throttling.
func ddosAlertBurstStream() <-chan SecurityAlert {
	burstAlerts := make(chan SecurityAlert, 2)
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #1"}
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #2"}
	close(burstAlerts)
	return burstAlerts
}
