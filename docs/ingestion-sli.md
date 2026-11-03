# Ingestion correlation without Kafka timing metadata

The ingestion start is obtained from the Ingress span by the separate
`insights-ingestion-correlator` service. Application Kafka messages use their
existing request IDs and trace context; publishers do not add ingestion origin
headers, timing extras, or start-time batch fields.

Preserve per-message/per-host identity and trace propagation through asynchronous
workers. Successful terminal processing and Inventory Views commits need explicit
`rh.ingestion.completed` trace events at verified boundaries, including successful
no-work paths. This completion-event integration is separate from removing the
retired Kafka metadata and is not yet validated for every application.

Where retained, origin decoders and native completion-reporting clients are
read-only compatibility for the previous prototype. Their duration reports require
legacy origin metadata; they are not the new trace-based correlator protocol.
Do not configure the new OTLP correlator as a `/v1/completions` endpoint.

The agreed target remains 99% within 60 seconds per application over a rolling
one-hour window, alerting after five consecutive minutes below 99%.
