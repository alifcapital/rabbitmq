package mqutils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alifcapital/rabbitmq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Publish(ctx context.Context, exchange, key string, msg amqp.Publishing, client *rabbitmq.Client) error {
	spanName := fmt.Sprintf("|publish|%s|%s", exchange, key)
	span, newCtx := opentracing.StartSpanFromContext(ctx, spanName)
	defer span.Finish()

	span.LogFields(log.String("message_id", msg.MessageId))

	bagItems := map[string]string{}
	if err := span.Tracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(bagItems)); err != nil {
		ext.LogError(span, err)
		return err
	}
	bagItemsJsonBytes, err := json.Marshal(bagItems)
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

// NewMessage assumes body to be json, for other formats write own factory function
func NewMessage(id string, body []byte) amqp.Publishing {
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
	}
}
