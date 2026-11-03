package kafka_test

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/redhatinsights/insights-ingress-go/internal/validators"
	kafkaValidator "github.com/redhatinsights/insights-ingress-go/internal/validators/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var _ = Describe("Announcement wire compatibility", func() {
	It("keeps service routing and trace context without ingestion timing metadata", func() {
		previous := otel.GetTextMapPropagator()
		otel.SetTextMapPropagator(propagation.TraceContext{})
		defer otel.SetTextMapPropagator(previous)
		parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
		ctx := trace.ContextWithSpanContext(context.Background(), parent)
		messages := make(chan validators.ValidationMessage, 1)
		validator := &kafkaValidator.Validator{ValidationProducerChannel: messages}
		validator.Validate(ctx, &validators.Request{RequestID: "upload-A", OrgID: "org", Service: "advisor"})
		message := <-messages
		Expect(message.Headers).To(HaveKeyWithValue("service", "advisor"))
		Expect(message.Headers).NotTo(HaveKey("rh-ingestion-v1"))
		extracted := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(message.Headers))
		Expect(trace.SpanContextFromContext(extracted).TraceID()).To(Equal(parent.TraceID()))
		var body map[string]interface{}
		Expect(json.Unmarshal(message.Message, &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("request_id", "upload-A"))
		Expect(body).NotTo(HaveKey("ingestion"))
		Expect(body).NotTo(HaveKey("started_at_ns"))
	})
})
