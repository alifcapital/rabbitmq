package mqutils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alifcapital/rabbitmq"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Publish(ctx context.Context, exchange, key string, v any, client *rabbitmq.Client) error {
	spanName := fmt.Sprintf("|publish|%s|%s", exchange, key)
	span, newCtx := opentracing.StartSpanFromContext(ctx, spanName)
	defer span.Finish()

	msg, err := NewMessage(uuid.NewString(), v)
	if err != nil {
		ext.LogError(span, err)
		return err
	}

	span.LogFields(log.String("message_id", msg.MessageId))

	bagItems := map[string]string{}
	if err := span.Tracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(bagItems)); err != nil {
		ext.LogError(span, err)
		return err
	}
	bagItemsJsonBytes, err := marshal(bagItems)
	if err != nil {
		ext.LogError(span, err)
		return err
	}

	msg.Headers[opentracingData] = string(bagItemsJsonBytes)
	if err := client.Publish(newCtx, exchange, key, false, false, msg); err != nil {
		ext.LogError(span, err)
		return err
	}
	return nil
}

func PublishMsg(ctx context.Context, exchange, key string, msg amqp.Publishing, client *rabbitmq.Client) error {
	spanName := fmt.Sprintf("|publish|%s|%s", exchange, key)
	span, newCtx := opentracing.StartSpanFromContext(ctx, spanName)
	defer span.Finish()

	span.LogFields(log.String("message_id", msg.MessageId))

	bagItems := map[string]string{}
	if err := span.Tracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(bagItems)); err != nil {
		ext.LogError(span, err)
		return err
	}
	bagItemsJsonBytes, err := marshal(bagItems)
	if err != nil {
		ext.LogError(span, err)
		return err
	}

	msg.Headers[opentracingData] = string(bagItemsJsonBytes)
	if err := client.Publish(newCtx, exchange, key, false, false, msg); err != nil {
		ext.LogError(span, err)
		return err
	}
	return nil
}

func NewMessage(id string, ptr any) (amqp.Publishing, error) {
	body, err := marshal(ptr)
	if err != nil {
		return amqp.Publishing{}, err
	}

	contentType := "text/json"
	contentEncoding := "utf-8"

	return amqp.Publishing{
		Headers:         map[string]interface{}{},
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		MessageId:       id,
		Timestamp:       time.Now(),
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
		Body:            body,
	}, nil
}

func marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
