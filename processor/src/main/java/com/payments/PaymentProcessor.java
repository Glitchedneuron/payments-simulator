package com.payments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.mq.*;
import com.ibm.mq.constants.CMQC;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.HashMap;
import java.util.Map;

public class PaymentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Tracer tracer;

    public static void main(String[] args) throws Exception {
        TracerSetup.init();
        tracer = GlobalOpenTelemetry.getTracer("payment-processor");

        String mqHost = System.getenv().getOrDefault("MQ_HOST", "ibmmq");
        int mqPort = Integer.parseInt(System.getenv().getOrDefault("MQ_PORT", "1414"));
        String mqChannel = System.getenv().getOrDefault("MQ_CHANNEL", "DEV.APP.SVRCONN");
        String mqUser = System.getenv().getOrDefault("MQ_USER", "app");
        String mqPassword = System.getenv().getOrDefault("MQ_PASSWORD", "passw0rd");
        String inQueue = System.getenv().getOrDefault("MQ_IN_QUEUE", "DEV.QUEUE.1");
        String outQueue = System.getenv().getOrDefault("MQ_OUT_QUEUE", "DEV.QUEUE.2");

        MQEnvironment.hostname = mqHost;
        MQEnvironment.port = mqPort;
        MQEnvironment.channel = mqChannel;
        MQEnvironment.userID = mqUser;
        MQEnvironment.password = mqPassword;

        MQQueueManager qMgr = connectWithRetry();
        System.out.println("Connected to IBM MQ");

        MQQueue reader = qMgr.accessQueue(inQueue,
                CMQC.MQOO_INPUT_AS_Q_DEF | CMQC.MQOO_FAIL_IF_QUIESCING);
        MQQueue writer = qMgr.accessQueue(outQueue,
                CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING);

        System.out.println("Listening on " + inQueue + ", forwarding to " + outQueue);

        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = CMQC.MQGMO_WAIT | CMQC.MQGMO_NO_SYNCPOINT | CMQC.MQGMO_CONVERT;
        gmo.waitInterval = 3000;

        while (true) {
            try {
                MQMessage inMsg = new MQMessage();
                reader.get(inMsg, gmo);

                String body = inMsg.readStringOfByteLength(inMsg.getDataLength());
                System.out.println("Received message: " + body);

                JsonNode root = MAPPER.readTree(body);
                Map<String, String> traceHeaders = new HashMap<>();
                if (root.has("traceContext")) {
                    root.get("traceContext").fields().forEachRemaining(e ->
                            traceHeaders.put(e.getKey(), e.getValue().asText()));
                }

                Context parentCtx = GlobalOpenTelemetry.getPropagators()
                        .getTextMapPropagator()
                        .extract(Context.current(), traceHeaders, MapGetter.INSTANCE);

                Span span = tracer.spanBuilder("process-payment")
                        .setParent(parentCtx)
                        .startSpan();

                try (var scope = span.makeCurrent()) {
                    JsonNode payment = root.get("payment");
                    if (payment != null) {
                        span.setAttribute("payment.id", payment.path("id").asText("unknown"));
                        span.setAttribute("payment.amount", payment.path("amount").asText("0"));
                    }

                    Map<String, String> outHeaders = new HashMap<>();
                    GlobalOpenTelemetry.getPropagators()
                            .getTextMapPropagator()
                            .inject(Context.current(), outHeaders, MapSetter.INSTANCE);

                    ObjectNode outPayload = MAPPER.createObjectNode();
                    outPayload.set("payment", payment);
                    outPayload.set("traceContext", MAPPER.valueToTree(outHeaders));

                    MQMessage outMsg = new MQMessage();
                    outMsg.format = CMQC.MQFMT_STRING;
                    outMsg.writeString(MAPPER.writeValueAsString(outPayload));

                    MQPutMessageOptions pmo = new MQPutMessageOptions();
                    pmo.options = CMQC.MQPMO_NO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID;

                    writer.put(outMsg, pmo);
                    span.addEvent("payment-forwarded");
                    System.out.println("Payment forwarded to " + outQueue);
                } finally {
                    span.end();
                }

            } catch (MQException e) {
                if (e.reasonCode == CMQC.MQRC_NO_MSG_AVAILABLE) {
                    // no message, loop
                } else {
                    System.err.println("MQ error: " + e.getMessage());
                }
            } catch (Exception e) {
                System.err.println("Processing error: " + e.getMessage());
            }
        }
    }

    private static MQQueueManager connectWithRetry() {
        int attempt = 0;
        while (true) {
            try {
                return new MQQueueManager("QM1");
            } catch (MQException e) {
                attempt++;
                long delay = Math.min(attempt * 3000L, 15000L);
                System.out.println("Connection attempt " + attempt + " failed: " + e.getMessage()
                        + ". Retrying in " + delay + "ms...");
                try { Thread.sleep(delay); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    static class MapGetter implements TextMapGetter<Map<String, String>> {
        static final MapGetter INSTANCE = new MapGetter();
        @Override public Iterable<String> keys(Map<String, String> map) { return map.keySet(); }
        @Override public String get(Map<String, String> map, String key) { return map.get(key); }
    }

    static class MapSetter implements TextMapSetter<Map<String, String>> {
        static final MapSetter INSTANCE = new MapSetter();
        @Override public void set(Map<String, String> map, String key, String value) { map.put(key, value); }
    }
}
