package mqutils

import "fmt"

// used for passing opentracing data between Publishers and Consumers
const opentracingData = "opentracing_data"

// NewExchangeName placeholder in format "{owner}.{name_you_desire}"
// example result: "subscriptions.primary"
func NewExchangeName(appName, exchangeName string) string {
	return fmt.Sprintf("%s.%s", appName, exchangeName)
}

// NewQueueName placeholder in format: "{owner}.{ENV}.{name_you_desire}"
// example result: "subscriptions.prod.primary"
// ENV specification help you keep testing queues separate from production,
// and also run any debugging listening on production data
func NewQueueName(appName, appEnv, queueName string) string {
	return fmt.Sprintf("%s.%s.%s", appName, appEnv, queueName)
}
