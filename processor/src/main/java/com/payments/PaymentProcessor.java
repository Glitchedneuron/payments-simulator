package com.payments;

import com.ibm.mq.*;
import com.ibm.mq.constants.CMQC;

public class PaymentProcessor {

    public static void main(String[] args) throws Exception {
        MQEnvironment.hostname = System.getenv().getOrDefault("MQ_HOST", "ibmmq");
        MQEnvironment.port     = Integer.parseInt(System.getenv().getOrDefault("MQ_PORT", "1414"));
        MQEnvironment.channel  = System.getenv().getOrDefault("MQ_CHANNEL", "DEV.APP.SVRCONN");
        MQEnvironment.userID   = System.getenv().getOrDefault("MQ_USER", "app");
        MQEnvironment.password = System.getenv().getOrDefault("MQ_PASSWORD", "passw0rd");

        String inQueue  = System.getenv().getOrDefault("MQ_IN_QUEUE",  "DEV.QUEUE.1");
        String outQueue = System.getenv().getOrDefault("MQ_OUT_QUEUE", "DEV.QUEUE.2");

        MQQueueManager qMgr = connectWithRetry();
        System.out.println("Connected to IBM MQ");

        MQQueue reader = qMgr.accessQueue(inQueue,  CMQC.MQOO_INPUT_AS_Q_DEF | CMQC.MQOO_FAIL_IF_QUIESCING);
        MQQueue writer = qMgr.accessQueue(outQueue, CMQC.MQOO_OUTPUT          | CMQC.MQOO_FAIL_IF_QUIESCING);
        System.out.println("Routing " + inQueue + " -> " + outQueue);

        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options      = CMQC.MQGMO_WAIT | CMQC.MQGMO_NO_SYNCPOINT | CMQC.MQGMO_CONVERT;
        gmo.waitInterval = 3000;

        while (true) {
            try {
                MQMessage msg = new MQMessage();
                reader.get(msg, gmo);

                String body = msg.readStringOfByteLength(msg.getDataLength());
                System.out.println("Forwarding: " + body);

                MQMessage out = new MQMessage();
                out.format = CMQC.MQFMT_STRING;
                out.writeString(body);

                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = CMQC.MQPMO_NO_SYNCPOINT | CMQC.MQPMO_NEW_MSG_ID;
                writer.put(out, pmo);

            } catch (MQException e) {
                if (e.reasonCode != CMQC.MQRC_NO_MSG_AVAILABLE) {
                    System.err.println("MQ error: " + e.getMessage());
                }
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
                System.out.println("Attempt " + attempt + " failed, retrying in " + delay + "ms...");
                try { Thread.sleep(delay); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }
}
