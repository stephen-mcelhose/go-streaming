package examples_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

// NetworkPacket represents a captured packet on an interface.
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

// Example_networkEngineer demonstrates how a Network Engineer uses go-streaming to
// filter malicious port scans, rate-limit outbound webhook notifications, and partition
// traffic by protocol to distinct monitoring queues.
func Example_networkEngineer() {
	ctx := context.Background()

	// Ingress raw packet stream
	packetCh := make(chan NetworkPacket, 4)
	packetCh <- NetworkPacket{SrcIP: "192.168.1.50", DstPort: 80, Protocol: "TCP", Bytes: 120}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 445, Protocol: "TCP", Bytes: 64} // SMB Probe
	packetCh <- NetworkPacket{SrcIP: "192.168.1.51", DstPort: 53, Protocol: "UDP", Bytes: 512}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 3389, Protocol: "TCP", Bytes: 64} // RDP Probe
	close(packetCh)

	// Sinks
	alertSink := make(chan SecurityAlert, 5)
	tcpSink := make(chan NetworkPacket, 5)
	udpSink := make(chan NetworkPacket, 5)

	inStream := streaming.NewChanStream(packetCh, nil)

	// 1. Partition packets by protocol
	routes := map[string]streaming.Sender[NetworkPacket]{
		"TCP": streaming.NewChanStream(nil, tcpSink),
		"UDP": streaming.NewChanStream(nil, udpSink),
	}

	_ = routing.Partition(ctx,
		func(p NetworkPacket) (string, error) { return p.Protocol, nil },
		routes,
		nil,
		inStream,
	)

	close(alertSink)
	close(tcpSink)
	close(udpSink)

	fmt.Printf("Received %d TCP packets and %d UDP packets\n", len(tcpSink), len(udpSink))
	// Output:
	// Received 3 TCP packets and 1 UDP packets
}

// Example_rateLimitedAlerting demonstrates how a Network Engineer prevents webhook flood during a DDoS attack.
func Example_networkEngineerRateLimitedAlerting() {
	ctx := context.Background()

	// High frequency alert storm
	burstAlerts := make(chan SecurityAlert, 3)
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood Detected from 10.0.0.1"}
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood Detected from 10.0.0.2"}
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood Detected from 10.0.0.3"}
	close(burstAlerts)

	throttledAlerts := make(chan SecurityAlert, 3)

	go func() {
		defer close(throttledAlerts)
		// Throttle allows at most 1 incident alert per 10ms to prevent overwhelming external pager
		_ = control.Throttle(ctx, 10*time.Millisecond,
			streaming.NewChanStream(burstAlerts, nil),
			streaming.NewChanStream(nil, throttledAlerts),
		)
	}()

	for alert := range throttledAlerts {
		fmt.Printf("[%s] Paged: %s\n", alert.Level, alert.Message)
	}

	// Output:
	// [CRITICAL] Paged: SYN Flood Detected from 10.0.0.1
	// [CRITICAL] Paged: SYN Flood Detected from 10.0.0.2
	// [CRITICAL] Paged: SYN Flood Detected from 10.0.0.3
}
