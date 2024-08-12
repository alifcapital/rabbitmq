package mqutils

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	trace2 "go.opentelemetry.io/otel/trace"

	"github.com/alifcapital/rabbitmq"
)

type Middleware func(next rabbitmq.IConsumer) rabbitmq.IConsumer

type PanicRecoveryCallback func(ctx context.Context, msg amqp.Delivery, recErr any)

func NewPanicRecoveryMiddleware(cb PanicRecoveryCallback) Middleware {
	return func(next rabbitmq.IConsumer) rabbitmq.IConsumer {
		return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			defer func() {
				if recErr := recover(); recErr != nil {
					if cb != nil {
						cb(ctx, msg, recErr)
					}
				}
			}()
			next.Consume(ctx, msg)
		})
	}
}

func NewTracerMiddleware() Middleware {
	return func(next rabbitmq.IConsumer) rabbitmq.IConsumer {
		return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			var span opentracing.Span
			var tracerCtx context.Context

			spanName := fmt.Sprintf("|consume|%s|%s", msg.Exchange, msg.RoutingKey)
			bagItemsJson, ok := msg.Headers[opentracingData].(string)
			if ok {
				bagItems := map[string]string{}
				_ = json.Unmarshal([]byte(bagItemsJson), &bagItems)
				spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(bagItems))
				if err != nil {
					span = opentracing.StartSpan(spanName, ext.SpanKindConsumer)
				} else {
					span = opentracing.StartSpan(spanName, ext.RPCServerOption(spanContext), ext.SpanKindConsumer)
				}
				tracerCtx = opentracing.ContextWithSpan(ctx, span)
			} else {
				span, tracerCtx = opentracing.StartSpanFromContext(ctx, spanName, ext.SpanKindConsumer)
			}
			defer span.Finish()

			span.LogFields(log.String("message_id", msg.MessageId))

			next.Consume(tracerCtx, msg)
		})
	}
}

// NewConsumerTraceLoggerMid example of Middleware which logs a message into traces
func NewConsumerTraceLoggerMid() Middleware {
	return func(next rabbitmq.IConsumer) rabbitmq.IConsumer {
		return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			span, newCtx := opentracing.StartSpanFromContext(ctx, "LOG_MESSAGE")
			defer span.Finish()

			span.LogFields(log.String("id", msg.MessageId))
			span.LogFields(log.String("body", string(msg.Body)))

			next.Consume(newCtx, msg)
		})
	}
}

// NewOpenTelemetryMiddleware openTelemetry tracer for rabbitmq
func NewOpenTelemetryMiddleware() Middleware {
	return func(next rabbitmq.IConsumer) rabbitmq.IConsumer {
		return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			var span trace2.Span
			var tracerCtx context.Context

			tracer := otel.Tracer("openTelemetry-amqp-tracer")

			spanName := fmt.Sprintf("|consume|%s|%s", msg.Exchange, msg.RoutingKey)
			bagItemsJson, ok := msg.Headers[opentracingData].(string)
			if ok {
				bagItems := map[string]string{}
				_ = json.Unmarshal([]byte(bagItemsJson), &bagItems)

				// Extract context from carrier
				propagator := propagation.TraceContext{}
				parentCtx := propagator.Extract(ctx, propagation.MapCarrier(bagItems))

				// Start span from extracted context
				tracerCtx, span = tracer.Start(parentCtx, spanName, trace2.WithSpanKind(trace2.SpanKindConsumer))
			} else {
				// Start a new span if no context is present
				tracerCtx, span = tracer.Start(ctx, spanName, trace2.WithSpanKind(trace2.SpanKindConsumer))
			}
			defer span.End()

			// Add attributes instead of log fields
			span.SetAttributes(attribute.String("message_id", msg.MessageId))

			// Consume the message
			next.Consume(tracerCtx, msg)
		})
	}
}

func NewConsumerOpenTelemetryTraceLoggerMid() Middleware {
	return func(next rabbitmq.IConsumer) rabbitmq.IConsumer {
		return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			tracer := otel.Tracer("openTelemetry-amqp-tracer") // Create tracer

			// Start a new span from the context
			newCtx, span := tracer.Start(ctx, "LOG_MESSAGE", trace2.WithSpanKind(trace2.SpanKindConsumer))
			defer span.End()

			// Add attributes to the span
			span.SetAttributes(
				attribute.String("id", msg.MessageId),
				attribute.String("body", string(msg.Body)),
			)

			// Consume the message with the new context
			next.Consume(newCtx, msg)
		})
	}
}
