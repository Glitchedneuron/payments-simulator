require('./tracer');

const mq = require('ibmmq');
const { trace, context, propagation } = require('@opentelemetry/api');
const MQC = mq.MQC;

const tracer = trace.getTracer('payment-consumer');

const connOptions = {
  hostname: process.env.MQ_HOST || 'ibmmq',
  port: parseInt(process.env.MQ_PORT || '1414'),
  channel: process.env.MQ_CHANNEL || 'DEV.APP.SVRCONN',
  user: process.env.MQ_USER || 'app',
  password: process.env.MQ_PASSWORD || 'passw0rd',
};

const QUEUE_NAME = process.env.MQ_QUEUE || 'DEV.QUEUE.2';

function getMessages(hConn, hObj) {
  const md = new mq.MQMD();
  const gmo = new mq.MQGMO();
  gmo.Options = MQC.MQGMO_NO_SYNCPOINT | MQC.MQGMO_WAIT | MQC.MQGMO_CONVERT;
  gmo.WaitInterval = 3000;

  mq.GetSync(hConn, hObj, md, gmo, 65536, (err, len, buf) => {
    if (err) {
      if (err.mqrc === MQC.MQRC_NO_MSG_AVAILABLE) {
        setImmediate(() => getMessages(hConn, hObj));
      } else {
        console.error('Get error:', err.message);
        setImmediate(() => getMessages(hConn, hObj));
      }
      return;
    }

    const raw = buf.toString('utf8', 0, len);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.log('Received (raw):', raw);
      setImmediate(() => getMessages(hConn, hObj));
      return;
    }

    const { payment, traceContext } = parsed;

    const parentCtx = propagation.extract(context.active(), traceContext || {});
    const span = tracer.startSpan('receive-payment', {}, parentCtx);

    context.with(trace.setSpan(parentCtx, span), () => {
      span.setAttributes({
        'payment.id': payment?.id,
        'payment.amount': payment?.amount,
        'payment.currency': payment?.currency,
        'payment.from': payment?.from,
        'payment.to': payment?.to,
      });

      console.log('=== Payment Received ===');
      console.log(JSON.stringify(payment, null, 2));
      console.log('=======================');

      span.addEvent('payment-printed');
      span.end();
    });

    setImmediate(() => getMessages(hConn, hObj));
  });
}

async function main() {
  const cno = new mq.MQCNO();
  cno.Options = MQC.MQCNO_CLIENT_BINDING;

  const cd = new mq.MQCD();
  cd.ConnectionName = `${connOptions.hostname}(${connOptions.port})`;
  cd.ChannelName = connOptions.channel;
  cno.ClientConn = cd;

  const csp = new mq.MQCSP();
  csp.UserId = connOptions.user;
  csp.Password = connOptions.password;
  cno.SecurityParms = csp;

  const connectWithRetry = (attempt = 1) => {
    mq.Connx('QM1', cno, (err, hConn) => {
      if (err) {
        const delay = Math.min(attempt * 3000, 15000);
        console.log(`MQ connection attempt ${attempt} failed: ${err.message}. Retrying in ${delay}ms...`);
        setTimeout(() => connectWithRetry(attempt + 1), delay);
        return;
      }

      console.log('Connected to IBM MQ, listening on', QUEUE_NAME);

      const od = new mq.MQOD();
      od.ObjectName = QUEUE_NAME;
      od.ObjectType = MQC.MQOT_Q;

      const oo = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_FAIL_IF_QUIESCING;

      mq.Open(hConn, od, oo, (openErr, hObj) => {
        if (openErr) {
          console.error('Open error:', openErr.message);
          mq.Disc(hConn, () => setTimeout(() => connectWithRetry(attempt + 1), 5000));
          return;
        }
        getMessages(hConn, hObj);
      });
    });
  };

  connectWithRetry();
}

main();
