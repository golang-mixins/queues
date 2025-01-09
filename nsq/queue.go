// Package nsq represents the interface queues implementation.
package nsq

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang-mixins/codec"
	"go.opencensus.io/plugin/ochttp/propagation/b3"
	"io"
	Log "log"
	"net/http"
	"sync"
	"time"

	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	"github.com/golang-mixins/queues"
	"github.com/nsqio/go-nsq"
)

const (
	// LogLevelDebug - nsq.LogLevelDebug.
	LogLevelDebug = nsq.LogLevelDebug
	// LogLevelInfo - nsq.LogLevelInfo.
	LogLevelInfo = nsq.LogLevelInfo
	// LogLevelWarning - nsq.LogLevelWarning.
	LogLevelWarning = nsq.LogLevelWarning
	// LogLevelError - nsq.LogLevelError.
	LogLevelError = nsq.LogLevelError
)

// Logger predetermines the consistency of the logging.
type Logger struct {
	Output io.Writer
	Level  int
}

// Validate validates Logger according to predefined rules.
func (l Logger) Validate() error {
	if l.Output == nil {
		return xerrors.New("log Output can't be nil")
	}
	switch l.Level {
	case int(LogLevelDebug):
		return nil
	case int(LogLevelInfo):
		return nil
	case int(LogLevelWarning):
		return nil
	case int(LogLevelError):
		return nil
	}
	return xerrors.New("log level not supported")
}

// Config delivers a set of settings for QueueManager implementation.
type Config struct {
	NsqConfig   *nsq.Config
	NsqD        string
	NsqLookupDs []string
	Channel     string
	Log         Logger
	StopTimeout time.Duration
	Marshaler   codec.Marshaler
	Unmarshaler codec.Unmarshaler
}

// Validate validates Config according to predefined rules.
func (c Config) Validate() error {
	switch {
	case c.NsqConfig == nil:
		return xerrors.New("NsqConfig can't be nil")
	case len(c.NsqD) == 0:
		return xerrors.New("NsqD can't be empty")
	case len(c.NsqLookupDs) == 0:
		return xerrors.New("NsqLookupDs can't be empty")
	case len(c.Channel) == 0:
		return xerrors.New("Channel can't be empty")
	case c.StopTimeout == 0:
		return xerrors.New("StopTimeout can't be empty")
	case c.Marshaler == nil:
		return xerrors.New("Marshaler can't be nil")
	case c.Unmarshaler == nil:
		return xerrors.New("Unmarshaler can't be nil")
	}
	if err := c.Log.Validate(); err != nil {
		return err
	}
	return nil
}

// Envelope transmitted in queues.
type Envelope struct {
	Magic  string          `json:"$magic"`
	Header http.Header     `json:"$header"`
	Body   json.RawMessage `json:"$body"`
}

// Message predetermines the consistency of the queues.Message implementation.
type Message struct {
	core *nsq.Message
	body []byte
	ctx  context.Context
	subj string
}

// Subject provides the message Subject.
func (m *Message) Subject() string {
	return m.subj
}

// Body provides the message body.
func (m *Message) Body() []byte {
	return m.body
}

// Timestamp provides the message timestamp.
func (m *Message) Timestamp() int64 {
	return m.core.Timestamp
}

// AttemptsCount provides the message attempts count.
func (m *Message) AttemptsCount() uint16 {
	return m.core.Attempts
}

// QueueAddr provides the address of the nsq daemon from which the message came.
func (m *Message) QueueAddr() string {
	return m.core.NSQDAddress
}

// HasResponded provides nsq.Message.HasResponded.
func (m *Message) HasResponded() bool {
	return m.core.HasResponded()
}

// DisableAutoResponse provides nsq.Message.DisableAutoResponse.
func (m *Message) DisableAutoResponse() {
	m.core.DisableAutoResponse()
}

// Touch provides nsq.Message.Touch.
func (m *Message) Touch() {
	m.core.Touch()
}

// RequeueWithoutBackoff provides nsq.Message.RequeueWithoutBackoff.
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.core.RequeueWithoutBackoff(delay)
}

// Finish provides nsq.Message.Finish.
func (m *Message) Finish() {
	m.core.Finish()
}

// Context provides message context.
func (m *Message) Context() context.Context {
	return m.ctx
}

// Subscription predetermines the consistency of the queues.Subscriber implementation.
type Subscription struct {
	mutex       *sync.RWMutex
	addrs       []string
	consumer    *nsq.Consumer
	stopTimeout time.Duration
}

// Unsubscribe disconnects the instance Subscription from nsq daemon.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	_, span := trace.StartSpan(ctx, "unsubscribe from nsq queue subscription")
	defer span.End()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	addrs := make([]string, 0, len(s.addrs))
	for i := range s.addrs {
		err := s.consumer.DisconnectFromNSQLookupd(s.addrs[i])
		if err != nil {
			addrs = append(addrs, s.addrs[i])
		}
	}

	if len(addrs) > 0 {
		return xerrors.Errorf("error unsubscribing from queue addrs %+v", addrs)
	}

	return nil
}

// Stop stops the instance Subscription.
func (s *Subscription) Stop(ctx context.Context) {
	_, span := trace.StartSpan(ctx, "stop nsq queue subscription")
	defer span.End()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	timer := time.NewTimer(s.stopTimeout)
	defer timer.Stop()

	s.consumer.ChangeMaxInFlight(0)

	s.consumer.Stop()
	select {
	case <-s.consumer.StopChan:
		return
	case <-timer.C:
		return
	}
}

// Queue predetermines the consistency of the queues.QueueManager implementation.
type Queue struct {
	mutex       *sync.RWMutex
	wg          *sync.WaitGroup
	config      *nsq.Config
	logger      *Log.Logger
	logLevel    nsq.LogLevel
	addrs       []string
	channel     string
	producer    *nsq.Producer
	subscribers []*Subscription
	stopTimeout time.Duration
	marshaler   codec.Marshaler
	unmarshaler codec.Unmarshaler
}

func (q *Queue) traceContextFromHeader(h http.Header) (trace.SpanContext, bool) {
	traceID, ok := b3.ParseTraceID(h.Get(b3.TraceIDHeader))
	if !ok {
		return trace.SpanContext{}, false
	}

	spanID, ok := b3.ParseSpanID(h.Get(b3.SpanIDHeader))
	if !ok {
		return trace.SpanContext{}, false
	}

	sampled, _ := b3.ParseSampled(h.Get(b3.SampledHeader))

	return trace.SpanContext{
		TraceID:      traceID,
		SpanID:       spanID,
		TraceOptions: sampled,
	}, true
}

// convertHandler encapsulates a handler call in the implementation required by nsg. Returns a handler wrapped.
func (q *Queue) convertHandler(handler queues.Handler, topic string) nsq.Handler {
	return nsq.HandlerFunc(
		func(msg *nsq.Message) error {
			name := fmt.Sprintf("init message from queue addrs %+v, channel '%s'", q.addrs, q.channel)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var span *trace.Span
			var body []byte
			env := new(Envelope)
			err := q.unmarshaler.Unmarshal(msg.Body, env)
			if err != nil || len(env.Magic) == 0 {
				ctx, span = trace.StartSpan(ctx, name)
				body = msg.Body
			} else {
				sc, ok := q.traceContextFromHeader(env.Header)
				if ok {
					ctx, span = trace.StartSpanWithRemoteParent(ctx, name, sc, trace.WithSpanKind(trace.SpanKindServer))
				} else {
					ctx, span = trace.StartSpan(ctx, name, trace.WithSpanKind(trace.SpanKindServer))
				}
				body = env.Body
			}
			defer span.End()
			return handler(
				&Message{
					msg,
					body,
					ctx,
					topic,
				})
		})
}

// Publish publishes a message to the queue.
func (q *Queue) Publish(ctx context.Context, topic string, body []byte) error {
	name := fmt.Sprintf("publish message to queue addr '%s', channel '%s'", q.producer.String(), q.channel)
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, name, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	sc := span.SpanContext()
	env := &Envelope{
		Body:   body,
		Magic:  "EDOv1",
		Header: make(http.Header),
	}

	env.Header.Set(b3.TraceIDHeader, hex.EncodeToString(sc.TraceID[:]))
	env.Header.Set(b3.SpanIDHeader, hex.EncodeToString(sc.SpanID[:]))
	env.Header.Set(
		b3.SampledHeader,
		func() string {
			if sc.IsSampled() {
				return "1"
			}
			return "0"
		}(),
	)

	msg, err := q.marshaler.Marshal(env)
	if err != nil {
		return xerrors.Errorf("error at marshal envelope message: %w", err)
	}

	err = q.producer.Publish(topic, msg)
	if err != nil {
		if xerrors.Is(err, nsq.ErrNotConnected) || xerrors.Is(err, nsq.ErrStopped) || xerrors.Is(err, nsq.ErrClosing) {
			return queues.NewConnectedError(xerrors.Errorf("error at publish topic '%s': %w", topic, err))
		}
		return xerrors.Errorf("error at publish topic '%s': %w", topic, err)
	}

	return nil
}

// Subscribe carries out accession to the nsq daemon.
func (q *Queue) Subscribe(ctx context.Context, topic string, handler queues.Handler) (queues.Subscriber, error) {
	_, span := trace.StartSpan(ctx, "subscribe to nsq topic")
	defer span.End()

	consumer, err := nsq.NewConsumer(topic, q.channel, q.config)
	if err != nil {
		return nil, xerrors.Errorf("error at creating consumer: %w", err)
	}

	if q.logger != nil {
		consumer.SetLogger(q.logger, q.logLevel)
	}

	consumer.AddConcurrentHandlers(q.convertHandler(handler, topic), q.config.MaxInFlight)
	consumer.ChangeMaxInFlight(q.config.MaxInFlight)
	
	err = consumer.ConnectToNSQLookupds(q.addrs)
	if err != nil {
		return nil, xerrors.Errorf("error at connecting consumer NSQLookupds %+v: %w", q.addrs, err)
	}

	subscriber := &Subscription{
		new(sync.RWMutex),
		q.addrs,
		consumer,
		q.stopTimeout,
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.subscribers = append(q.subscribers, subscriber)
	return subscriber, nil
}

// Unsubscribe disconnects all subscribers from the nsq daemon.
func (q *Queue) Unsubscribe(ctx context.Context) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "unsubscribe from all nsq queue subscription")
	defer span.End()

	q.mutex.Lock()
	defer q.mutex.Unlock()

	for _, s := range q.subscribers {
		q.wg.Add(1)
		go func(s *Subscription) {
			defer q.wg.Done()
			s.Unsubscribe(ctx)
		}(s)
	}
	q.wg.Wait()
}

func (q *Queue) stopProducer(timeout time.Duration) {
	shutdown := make(chan struct{})

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		q.producer.Stop()
		shutdown <- struct{}{}
		close(shutdown)
	}()

	select {
	case <-shutdown:
		return
	case <-timer.C:
		return
	}
}

// Stop stops producer and all instances of subscribers.
func (q *Queue) Stop(ctx context.Context) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "stop from all nsq queue subscription")
	defer span.End()

	q.mutex.Lock()
	defer q.mutex.Unlock()

	start := time.Now()

	for _, s := range q.subscribers {
		q.wg.Add(1)

		go func(s *Subscription) {
			defer q.wg.Done()
			s.Stop(ctx)
		}(s)
	}
	q.wg.Wait()

	q.stopProducer(q.stopTimeout - time.Since(start))
}

// Ping - ping to producer,
func (q *Queue) Ping(ctx context.Context) error {
	_, span := trace.StartSpan(ctx, "ping queue producer")
	defer span.End()

	err := q.producer.Ping()
	if err != nil {
		return queues.NewConnectedError(xerrors.Errorf("error by ping producer: %w", err))
	}

	return nil
}

// New - QueueManager constructor.
func New(cfg Config) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	log := Log.New(cfg.Log.Output, "nsq queue logger: ", Log.LstdFlags|Log.Lmicroseconds|Log.Llongfile|Log.Lshortfile)

	producer, err := nsq.NewProducer(cfg.NsqD, cfg.NsqConfig)
	if err != nil {
		return nil, xerrors.Errorf("error at create producer: %w", err)
	}
	producer.SetLogger(log, nsq.LogLevel(cfg.Log.Level))

	queue := &Queue{
		new(sync.RWMutex),
		new(sync.WaitGroup),
		cfg.NsqConfig,
		log,
		nsq.LogLevel(cfg.Log.Level),
		cfg.NsqLookupDs,
		cfg.Channel,
		producer,
		make([]*Subscription, 0),
		cfg.StopTimeout,
		cfg.Marshaler,
		cfg.Unmarshaler,
	}

	return queue, nil
}
