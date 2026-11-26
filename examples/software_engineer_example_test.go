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

// UserSignupCommand represents an incoming request sent to our application.
type UserSignupCommand struct {
	UserID string
	Email  string
	Tier   string
}

// ProvisionJob represents a task ready to be sent to a background worker or external service.
type ProvisionJob struct {
	JobID     string
	Recipient string
	Tier      string
}

// simulatedProvisionService simulates an external service that occasionally experiences temporary errors.
type simulatedProvisionService struct {
	attempts map[string]int
}

func (s *simulatedProvisionService) Send(ctx context.Context, job ProvisionJob) error {
	s.attempts[job.JobID]++
	// Permanent error: this request has invalid configuration and will always fail
	if job.JobID == "job_usr_corrupt" {
		return errors.New("terminal: invalid customer tier specification")
	}
	// Temporary error: fails on the first attempt, then succeeds on retry
	if job.JobID == "job_usr_transient" && s.attempts[job.JobID] < 2 {
		return errors.New("transient: connection pool busy, please retry")
	}
	return nil
}

// Example_softwareEngineer demonstrates how to build a reliable background task pipeline:
// 1. Reads incoming signup commands from an input stream.
// 2. Converts each command into a structured background job using transformations.Map.
// 3. Sends jobs using control.RetrySender, which automatically retries failed requests with increasing delays.
// 4. Automatically forwards jobs that still fail after 3 attempts to a Dead Letter Queue (DLQ).
func Example_softwareEngineer() {
	ctx := context.Background()

	// Ingress stream containing sample signup requests
	commands := userSignupCommandStream()

	jobQueue := make(chan ProvisionJob, 3)
	dlqSink := make(chan ProvisionJob, 3)

	inStream := streaming.NewChanStream(commands, nil)
	outStream := streaming.NewChanStream(nil, jobQueue)

	// Step 1: Transform user signup commands into background jobs
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

	// Step 2: Configure automatic retries with a fallback destination for permanent failures
	serviceBackend := &simulatedProvisionService{attempts: make(map[string]int)}
	retryPolicy := control.RetryPolicy[ProvisionJob]{
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2.0,                                   // Double the wait time after each failed attempt
		FallbackSender: streaming.NewChanStream(nil, dlqSink), // Send items here if all retries fail
	}
	resilientSender := control.NewRetrySender[ProvisionJob](serviceBackend, retryPolicy)

	// Step 3: Read jobs from the queue and send them to the downstream service
	for job := range jobQueue {
		_ = resilientSender.Send(ctx, job)
		if serviceBackend.attempts[job.JobID] > 0 && job.JobID != "job_usr_corrupt" {
			fmt.Printf("[DISPATCH SUCCESS] %s (%s, tier: %s)\n", job.JobID, job.Recipient, job.Tier)
		}
	}

	// Step 4: Check the Dead Letter Queue for any jobs that could not be processed
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

// userSignupCommandStream provides test commands covering three common real-world outcomes:
// 1. usr_ok: Standard valid request that succeeds immediately.
// 2. usr_transient: Request that encounters a temporary service glitch on first try, then succeeds on retry #2.
// 3. usr_corrupt: Invalid request that fails all 3 attempts and is safely saved into the Dead Letter Queue.
func userSignupCommandStream() <-chan UserSignupCommand {
	commands := make(chan UserSignupCommand, 3)
	commands <- UserSignupCommand{UserID: "usr_ok", Email: "alice@acme.com", Tier: "pro"}
	commands <- UserSignupCommand{UserID: "usr_transient", Email: "bob@acme.com", Tier: "enterprise"}
	commands <- UserSignupCommand{UserID: "usr_corrupt", Email: "bad@acme.com", Tier: "unknown"}
	close(commands)
	return commands
}
