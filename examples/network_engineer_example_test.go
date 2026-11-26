package examples_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
	"github.com/stephen-mcelhose/go-streaming/operators/routing"
)

// NetworkPacket represents a single packet observed on a network interface.
type NetworkPacket struct {
	SrcIP    string
	DstPort  int
	Protocol string
	Bytes    int
}

// SecurityAlert represents a generated notification about suspicious activity.
type SecurityAlert struct {
	Level   string
	Message string
}

// Example_networkEngineer demonstrates how to process live network traffic:
// 1. Reads incoming network packets from an input stream.
// 2. Splits packets into separate processing paths based on protocol (TCP vs UDP) using routing.Partition.
// 3. Copies critical security alerts to two destinations at once using routing.BroadcastBestEffort.
// 4. Limits the rate of outbound notifications using control.Throttle so teams are not overwhelmed during traffic spikes.
func Example_networkEngineer() {
	ctx := context.Background()

	// Ingress stream of observed network packets
	packetCh := networkPacketStream()

	// Step 1: Split packets into separate TCP and UDP queues
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

	// Step 2: Send copies of an alert to multiple systems (such as a log archive and an alert dashboard).
	// BroadcastBestEffort ensures that if one logging target is slow or unavailable, it does not block the other.
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

	// Step 3: Rate-limit alerts to send at most one notification every 10 milliseconds.
	// This helps prevent notification spam during sudden traffic surges.
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

// networkPacketStream returns a sample stream of captured network packets.
// It contains web traffic on port 80, DNS queries on port 53, and connection attempts on ports 445 and 3389.
func networkPacketStream() <-chan NetworkPacket {
	packetCh := make(chan NetworkPacket, 4)
	packetCh <- NetworkPacket{SrcIP: "192.168.1.50", DstPort: 80, Protocol: "TCP", Bytes: 120}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 445, Protocol: "TCP", Bytes: 64} // Port probe
	packetCh <- NetworkPacket{SrcIP: "192.168.1.51", DstPort: 53, Protocol: "UDP", Bytes: 512}
	packetCh <- NetworkPacket{SrcIP: "10.0.0.99", DstPort: 3389, Protocol: "TCP", Bytes: 64} // Port probe
	close(packetCh)
	return packetCh
}

// securityAlertStream provides a sample urgent alert that needs to be sent to multiple logging systems.
func securityAlertStream() <-chan SecurityAlert {
	alertCh := make(chan SecurityAlert, 1)
	alertCh <- SecurityAlert{Level: "CRITICAL", Message: "Port Scan Detected from 10.0.0.99"}
	close(alertCh)
	return alertCh
}

// ddosAlertBurstStream produces multiple alerts in rapid succession to test rate limiting.
func ddosAlertBurstStream() <-chan SecurityAlert {
	burstAlerts := make(chan SecurityAlert, 2)
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #1"}
	burstAlerts <- SecurityAlert{Level: "CRITICAL", Message: "SYN Flood #2"}
	close(burstAlerts)
	return burstAlerts
}
