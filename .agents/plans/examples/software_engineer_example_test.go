package examples_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

// UserSignupEvent represents an incoming event from an API or webhook.
type UserSignupEvent struct {
	UserID    string
	Email     string
	Plan      string
	Timestamp time.Time
}

// ProvisionJob represents an asynchronous downstream work order.
type ProvisionJob struct {
	JobID     string
	TenantID  string
	Email     string
	Tier      string
	CreatedAt time.Time
}

// mockRemoteProvisionService simulates an external provisioning microservice.
type mockRemoteProvisionService struct {
	attempts map[string]int
}

func (s *mockRemoteProvisionService) Send(ctx context.Context, job ProvisionJob) error {
	s.attempts[job.JobID]++
	// Fail job "usr_fail" permanently to trigger DLQ
	if job.TenantID == "usr_fail" {
		return errors.New("provisioning API unreachable")
	}
	// Fail job "usr_transient" once, then succeed on retry
	if job.TenantID == "usr_transient" && s.attempts[job.JobID] < 2 {
		return errors.New("transient database lock")
	}
	return nil
}

// Example_softwareEngineerWorkflow demonstrates how a Backend / Distributed Systems Engineer
// constructs an end-to-end resilient worker pipeline:
// 1. Ingests domain events from a receiver
// 2. Maps/validates events into downstream provisioning jobs
// 3. Delivers jobs via a RetrySender with exponential backoff
// 4. Automatically forwards exhausted failures to a Dead Letter Queue (DLQ)
func Example_softwareEngineerWorkflow() {
	ctx := context.Background()

	// Ingress stream of user signups
	eventCh := make(chan UserSignupEvent, 3)
	eventCh <- UserSignupEvent{UserID: "usr_ok", Email: "ok@corp.io", Plan: "pro", Timestamp: time.Now()}
	eventCh <- UserSignupEvent{UserID: "usr_transient", Email: "retry@corp.io", Plan: "enterprise", Timestamp: time.Now()}
	eventCh <- UserSignupEvent{UserID: "usr_fail", Email: "fail@corp.io", Plan: "free", Timestamp: time.Now()}
	close(eventCh)

	// Intermediates & Sinks
	jobQueue := make(chan ProvisionJob, 3)
	dlqSink := make(chan ProvisionJob, 3)

	inStream := streaming.NewChanStream(eventCh, nil)
	jobStream := streaming.NewChanStream(nil, jobQueue)

	// Step 1: Transform domain signup events into provisioning work orders
	go func() {
		defer close(jobQueue)
		_ = transformations.Map(ctx, func(ctx context.Context, ev UserSignupEvent) (ProvisionJob, error) {
			return ProvisionJob{
				JobID:     "job_" + ev.UserID,
				TenantID:  ev.UserID,
				Email:     ev.Email,
				Tier:      ev.Plan,
				CreatedAt: ev.Timestamp,
			}, nil
		}, inStream, jobStream)
	}()

	// Step 2: Configure resilient downstream delivery with RetryPolicy and DLQ Fallback
	remoteService := &mockRemoteProvisionService{attempts: make(map[string]int)}
	retryPolicy := control.RetryPolicy[ProvisionJob]{
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2.0,
		FallbackSender: streaming.NewChanStream(nil, dlqSink), // Routes to DLQ when 3 attempts fail
	}
	resilientSender := control.NewRetrySender[ProvisionJob](remoteService, retryPolicy)

	// Step 3: Consume jobs and dispatch
	for job := range jobQueue {
		err := resilientSender.Send(ctx, job)
		if err == nil {
			fmt.Printf("Job %s: Successfully provisioned for %s\n", job.JobID, job.Email)
		}
	}

	// Step 4: Inspect DLQ for exhausted failures
	close(dlqSink)
	for deadJob := range dlqSink {
		fmt.Printf("DLQ Alert: Job %s for %s routed to DLQ after retry exhaustion\n", deadJob.JobID, deadJob.Email)
	}

	// Output:
	// Job job_usr_ok: Successfully provisioned for ok@corp.io
	// Job job_usr_transient: Successfully provisioned for retry@corp.io
	// DLQ Alert: Job job_usr_fail for fail@corp.io routed to DLQ after retry exhaustion
}
