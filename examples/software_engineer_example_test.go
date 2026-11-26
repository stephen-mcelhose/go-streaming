package examples_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stephen-mcelhose/go-streaming"
	"github.com/stephen-mcelhose/go-streaming/operators/control"
	"github.com/stephen-mcelhose/go-streaming/operators/transformations"
)

type UserSignupCommand struct {
	UserID string
	Email  string
}

type WelcomeEmailJob struct {
	Recipient string
	Subject   string
}

// Example_softwareEngineer demonstrates how a Software Engineer builds an asynchronous
// command processor with error-handling, transformation into notification jobs, and retry with fallback DLQ.
func Example_softwareEngineer() {
	ctx := context.Background()

	// Ingress command stream (e.g. from HTTP handler or service channel)
	commands := make(chan UserSignupCommand, 2)
	commands <- UserSignupCommand{UserID: "usr_1", Email: "alex@example.com"}
	commands <- UserSignupCommand{UserID: "usr_2", Email: "sam@example.com"}
	close(commands)

	emailJobs := make(chan WelcomeEmailJob, 2)
	dlqSink := make(chan WelcomeEmailJob, 2)

	inStream := streaming.NewChanStream(commands, nil)
	outStream := streaming.NewChanStream(nil, emailJobs)

	// Transform user signup commands into templated welcome email jobs
	go func() {
		defer close(emailJobs)
		_ = transformations.Map(ctx, func(ctx context.Context, cmd UserSignupCommand) (WelcomeEmailJob, error) {
			return WelcomeEmailJob{
				Recipient: cmd.Email,
				Subject:   fmt.Sprintf("Welcome to the platform, %s!", cmd.UserID),
			}, nil
		}, inStream, outStream)
	}()

	// Resilient email dispatcher with retry policy and DLQ fallback
	retryPolicy := control.RetryPolicy[WelcomeEmailJob]{
		MaxAttempts:    2,
		InitialBackoff: 5 * time.Millisecond,
		FallbackSender: streaming.NewChanStream(nil, dlqSink),
	}
	emailSender := control.NewRetrySender[WelcomeEmailJob](streaming.NewChanStream(nil, make(chan WelcomeEmailJob, 10)), retryPolicy)

	for job := range emailJobs {
		_ = emailSender.Send(ctx, job)
		fmt.Printf("Queued email to %s: '%s'\n", job.Recipient, job.Subject)
	}

	// Output:
	// Queued email to alex@example.com: 'Welcome to the platform, usr_1!'
	// Queued email to sam@example.com: 'Welcome to the platform, usr_2!'
}
