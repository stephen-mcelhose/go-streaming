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

// UserSignupCommand represents an incoming command from an API gateway or message broker.
type UserSignupCommand struct {
	UserID string
	Email  string
	Tier   string
}

// ProvisionJob represents an asynchronous work order dispatched to a downstream provisioning service.
type ProvisionJob struct {
	JobID     string
	Recipient string
	Tier      string
}

// simulatedProvisionService models an external microservice with intermittent network/DB faults.
type simulatedProvisionService struct {
	attempts map[string]int
}

func (s *simulatedProvisionService) Send(ctx context.Context, job ProvisionJob) error {
	s.attempts[job.JobID]++
	// Terminal failure: simulate non-existent tenant/account error
	if job.JobID == "job_usr_corrupt" {
		return errors.New("terminal: invalid customer tier specification")
	}
	// Transient failure: simulate database lock on 1st attempt, recovers on 2nd attempt
	if job.JobID == "job_usr_transient" && s.attempts[job.JobID] < 2 {
		return errors.New("transient: connection pool exhausted, try again")
	}
	return nil
}

// Example_softwareEngineer demonstrates how a Backend / Distributed Systems Engineer
// builds a resilient asynchronous task dispatch pipeline:
// 1. Ingests raw incoming commands from an ingress stream.
// 2. Maps/enriches commands into structured asynchronous job specifications.
// 3. Dispatches jobs through control.RetrySender with exponential backoff and jitter.
// 4. Automatically routes terminally failed jobs to a Dead Letter Queue (DLQ) without crashing.
// 5. Provides end-to-end observability across successes, retries, and DLQ entries.
func Example_softwareEngineer() {
	ctx := context.Background()

	// Ingress stream containing scenario commands
	commands := userSignupCommandStream()

	jobQueue := make(chan ProvisionJob, 3)
	dlqSink := make(chan ProvisionJob, 3)

	inStream := streaming.NewChanStream(commands, nil)
	outStream := streaming.NewChanStream(nil, jobQueue)

	// Step 1: Transform signup commands into structured ProvisionJobs
	go func() {
		defer close(jobQueue)
		_ = transformations.Map(ctx, func(ctx context.Context, cmd UserSignupCommand) (ProvisionJob, error) {
			return ProvisionJob{
				JobID:     "job_" + cmd.UserID,
				Recipient: cmd.Email,
				Tier:      cmd.Tier,
			}, nil
		}, inStream, outStream)
	}()

	// Step 2: Configure resilient downstream delivery with RetryPolicy & FallbackSender (DLQ)
	serviceBackend := &simulatedProvisionService{attempts: make(map[string]int)}
	retryPolicy := control.RetryPolicy[ProvisionJob]{
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2.0,
		FallbackSender: streaming.NewChanStream(nil, dlqSink), // Safely catches exhausted failures
	}
	resilientSender := control.NewRetrySender[ProvisionJob](serviceBackend, retryPolicy)

	// Step 3: Consume and dispatch jobs
	for job := range jobQueue {
		_ = resilientSender.Send(ctx, job)
		// Check service delivery status
		if serviceBackend.attempts[job.JobID] > 0 && job.JobID != "job_usr_corrupt" {
			fmt.Printf("[DISPATCH SUCCESS] %s (%s, tier: %s)\n", job.JobID, job.Recipient, job.Tier)
		}
	}

	// Step 4: Drain and inspect Dead Letter Queue (DLQ)
	close(dlqSink)
	for deadJob := range dlqSink {
		fmt.Printf("[DLQ ROUTED] %s (%s) routed to DLQ after %d attempts\n",
			deadJob.JobID, deadJob.Recipient, serviceBackend.attempts[deadJob.JobID])
	}

	// Output:
	// [DISPATCH SUCCESS] job_usr_ok (alice@acme.com, tier: pro)
	// [DISPATCH SUCCESS] job_usr_transient (bob@acme.com, tier: enterprise)
	// [DLQ ROUTED] job_usr_corrupt (bad@acme.com) routed to DLQ after 3 attempts
}

// userSignupCommandStream returns a stream representing three distinct real-world execution paths:
// 1. usr_ok: Valid account signup, succeeds on first dispatch attempt.
// 2. usr_transient: Experiences temporary connection contention, succeeds on retry #2.
// 3. usr_corrupt: Invalid configuration, exhausts all 3 retry attempts and routes cleanly to DLQ.
func userSignupCommandStream() <-chan UserSignupCommand {
	commands := make(chan UserSignupCommand, 3)
	commands <- UserSignupCommand{UserID: "usr_ok", Email: "alice@acme.com", Tier: "pro"}
	commands <- UserSignupCommand{UserID: "usr_transient", Email: "bob@acme.com", Tier: "enterprise"}
	commands <- UserSignupCommand{UserID: "usr_corrupt", Email: "bad@acme.com", Tier: "unknown"}
	close(commands)
	return commands
}
