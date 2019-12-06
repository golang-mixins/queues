// Package queues presents interface (and its implementation sets) of a queues.
package queues

import (
	"context"
	"fmt"
	"golang.org/x/xerrors"
	"time"
)

// ConnectedError typifies error "race condition".
type ConnectedError struct {
	cause error
	frame xerrors.Frame
}

// Error returns an error message.
func (ce *ConnectedError) Error() string {
	if ce.cause == nil {
		return ""
	}

	return ce.cause.Error()
}

// Unwrap satisfies the interface xerrors.Wrapper.
func (ce *ConnectedError) Unwrap() error {
	return ce.cause
}

// FormatError satisfies the interface xerrors.Formatter.
func (ce *ConnectedError) FormatError(p xerrors.Printer) (next error) {
	p.Print("init queue connection error")
	ce.frame.Format(p)

	return ce.Unwrap()
}

// Format satisfies the interface fmt.Formatter.
func (ce *ConnectedError) Format(s fmt.State, v rune) { xerrors.FormatError(ce, s, v) }

// NewConnectedError - constructor ConnectedError based on the cause of the error.
func NewConnectedError(cause error) *ConnectedError {
	return &ConnectedError{
		cause: cause,
		frame: xerrors.Caller(1),
	}
}

// Message predetermines the message type for handler.
type Message interface {
	// Body - provides the message body.
	Body() []byte
	// Timestamp - provides the message timestamp.
	Timestamp() int64
	// AttemptsCount provides message attempts count.
	AttemptsCount() uint16
	// QueueAddr - provides queue addr.
	QueueAddr() string
	// HasResponded - indicates whether or not this message has been responded to.
	HasResponded() bool
	// DisableAutoResponse - disables the automatic response that would normally be sent when a handler.
	DisableAutoResponse()
	// Touch - sends a touch command.
	Touch()
	// RequeueWithoutBackoff - sent this message, using the supplied delay.
	RequeueWithoutBackoff(delay time.Duration)
	// Finish - sends a finish command.
	Finish()
	// Context - provides message context.
	Context() context.Context
	// Subject - provides the message Subject.
	Subject() string
}

// Subscriber predetermines the type of the basic function of the subscriber queue.
type Subscriber interface {
	// Unsubscribe - unsubscribe Subscriber.
	Unsubscribe(ctx context.Context) error
	// Stop - stop Subscriber.
	Stop(ctx context.Context)
}

// Handler predetermines the type of the basic function of the message handler.
type Handler func(msg Message) error

// Publisher predetermines the type of the basic function of the publish action.
type Publisher interface {
	// Publish - publish message.
	Publish(ctx context.Context, subj string, body []byte) error
}

// Pinger predetermines the type of the basic function of the ping action.
type Pinger interface {
	// Ping - ping queue,
	Ping(ctx context.Context) error
}

// QueueManager provides an interface for managing queue.
type QueueManager interface {
	Publisher
	Pinger
	// Subscribe - subscribe to subj. Return Subscriber.
	Subscribe(ctx context.Context, subj string, handler Handler) (Subscriber, error)
	// Unsubscribe - unsubscribe Queue.
	Unsubscribe(ctx context.Context)
	// Stop - stop Queue.
	Stop(ctx context.Context)
}
