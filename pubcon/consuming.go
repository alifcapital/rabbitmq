package pubcon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alifcapital/rabbitmq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

const opentracingData = "opentracing_data"

type ConsumerMiddleware func(next rabbitmq.IConsumer) rabbitmq.IConsumer

type PanicRecoveryCallback func(ctx context.Context, msg amqp.Delivery, recErr any)

func ConsumerPanicRecovery(cb PanicRecoveryCallback) ConsumerMiddleware {
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

func ConsumerTracer() ConsumerMiddleware {
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
			span.LogFields(log.Int("priority", int(msg.Priority)))

			next.Consume(tracerCtx, msg)
		})
	}
}

func CombineConsumerMiddlewares(consumer rabbitmq.IConsumer, mids ...ConsumerMiddleware) rabbitmq.IConsumer {
	midsLen := len(mids)
	if midsLen == 0 {
		return consumer
	}
	firstChain := consumer
	for i := midsLen; i > 0; i-- {
		firstChain = mids[i-1](firstChain)
	}
	return firstChain
}
