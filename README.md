# Payments Simulator

A simple end-to-end payment flow using IBM MQ with distributed tracing via OpenTelemetry + Jaeger.

## Flow

```
[producer (Node.js)] --> DEV.QUEUE.1 --> [processor (Java)] --> DEV.QUEUE.2 --> [consumer (Node.js)]
```

1. **producer** — generates a payment and puts it on `DEV.QUEUE.1`
2. **processor** — reads from `DEV.QUEUE.1`, processes, forwards to `DEV.QUEUE.2`
3. **consumer** — reads from `DEV.QUEUE.2` and prints the payment

Trace context is propagated across all hops using W3C TraceContext headers embedded in the MQ message payload.

## Run

```bash
docker compose up --build
```

## View Traces

Open Jaeger UI: http://localhost:16686

## Re-send a payment

The producer exits after sending one payment. To send another:

```bash
docker compose run --rm producer
```
