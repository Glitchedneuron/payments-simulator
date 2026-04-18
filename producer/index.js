require('./tracer');

const mq = require('ibmmq');
const { trace, context, propagation } = require('@opentelemetry/api');
const MQC = mq.MQC;

const tracer = trace.getTracer('payment-producer');

const connOptions = {
  hostname: process.env.MQ_HOST || 'ibmmq',
  port: parseInt(process.env.MQ_PORT || '1414'),
  channel: process.env.MQ_CHANNEL || 'DEV.APP.SVRCONN',
  user: process.env.MQ_USER || 'app',
  password: process.env.MQ_PASSWORD || 'passw0rd',
};

function sendPayment(hConn, payment, traceHeaders) {
  return new Promise((resolve, reject) => {
    const od = new mq.MQOD();
    od.ObjectName = process.env.MQ_QUEUE || 'DEV.QUEUE.1';
    od.ObjectType = MQC.MQOT_Q;

    const pmo = new mq.MQPMO();
    pmo.Options = MQC.MQPMO_NO_SYNCPOINT | MQC.MQPMO_NEW_MSG_ID | MQC.MQPMO_NEW_CORREL_ID;

    const md = new mq.MQMD();
    md.Format = MQC.MQFMT_STRING;
    md.CorrelId = Buffer.alloc(24);

    const payload = JSON.stringify({ payment, traceContext: traceHeaders });

    mq.Put(hConn, od, md, pmo, payload, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function main() {
  const span = tracer.startSpan('generate-and-submit-payment');
  const ctx = trace.setSpan(context.active(), span);

  const traceHeaders = {};
  propagation.inject(ctx, traceHeaders);

  const payment = {
    id: `PAY-${Date.now()}`,
    amount: (Math.random() * 1000).toFixed(2),
    currency: 'USD',
    from: 'Alice',
    to: 'Bob',
    timestamp: new Date().toISOString(),
  };

  console.log('Generating payment:', payment);

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

  try {
    await new Promise((resolve, reject) => {
      mq.Connx('QM1', cno, (err, hConn) => {
        if (err) return reject(err);
        span.addEvent('connected-to-mq');

        context.with(ctx, async () => {
          try {
            await sendPayment(hConn, payment, traceHeaders);
            span.addEvent('payment-submitted', { 'payment.id': payment.id });
            console.log(`Payment ${payment.id} submitted to MQ queue`);
          } catch (putErr) {
            span.recordException(putErr);
            console.error('Error putting message:', putErr);
          } finally {
            mq.Disc(hConn, () => resolve());
          }
        });
      });
    });
  } catch (err) {
    span.recordException(err);
    console.error('Connection error:', err);
  } finally {
    span.end();
  }

  setTimeout(() => process.exit(0), 2000);
}

main();
