import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const CUSTOMER_ID = '6ba7b810-9dad-11d1-80b4-00c04fd43023';

const errors = new Counter('order_errors');
const failRate = new Rate('failed_requests');

// Переопределяется флагами CLI: --vus / --duration / --stage
export const options = {
  vus: 5,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<800'],
    failed_requests: ['rate<0.05'],
  },
};

function jsonHeaders() {
  return { headers: { 'Content-Type': 'application/json' } };
}

function createOrderPayload() {
  return JSON.stringify({
    customer_id: CUSTOMER_ID,
    status: 'pending',
    total_amount: 1500 + Math.floor(Math.random() * 500),
    currency: 'USD',
    items: [
      { sku: 'A1', qty: 1 + Math.floor(Math.random() * 3), price: 500 },
      { sku: 'B2', qty: 1, price: 500 },
    ],
  });
}

export default function () {
  let orderId = null;

  group('create', () => {
    const res = http.post(`${BASE_URL}/order`, createOrderPayload(), jsonHeaders());
    const ok = check(res, {
      'create status 201|202': (r) => r.status === 201 || r.status === 202,
      'create has id': (r) => r.body && String(r.body).length > 0,
    });
    failRate.add(!ok);
    if (!ok) {
      errors.add(1);
      return;
    }
    orderId = String(res.body).trim().replaceAll('"', '');
  });

  if (!orderId) {
    sleep(0.3);
    return;
  }

  group('get one', () => {
    const res = http.get(`${BASE_URL}/orders/${orderId}`);
    const ok = check(res, { 'get status 200': (r) => r.status === 200 });
    failRate.add(!ok);
    if (!ok) errors.add(1);
  });

  group('list', () => {
    const res = http.get(`${BASE_URL}/orders`);
    const ok = check(res, { 'list status 200': (r) => r.status === 200 });
    failRate.add(!ok);
    if (!ok) errors.add(1);
  });

  group('events', () => {
    const res = http.get(`${BASE_URL}/order-events`);
    const ok = check(res, { 'events status 200': (r) => r.status === 200 });
    failRate.add(!ok);
    if (!ok) errors.add(1);
  });

  group('update', () => {
    const body = JSON.stringify({
      id: orderId,
      customer_id: CUSTOMER_ID,
      status: 'paid',
      total_amount: 2000,
      currency: 'USD',
      items: [{ sku: 'A1', qty: 2, price: 1000 }],
    });
    const res = http.patch(`${BASE_URL}/orders`, body, jsonHeaders());
    const ok = check(res, { 'update status 200': (r) => r.status === 200 });
    failRate.add(!ok);
    if (!ok) errors.add(1);
  });

  if (Math.random() < 0.3) {
    group('soft delete', () => {
      const res = http.del(`${BASE_URL}/orders/${orderId}`);
      const ok = check(res, { 'delete status 200': (r) => r.status === 200 });
      failRate.add(!ok);
      if (!ok) errors.add(1);
    });
  }

  sleep(0.2);
}
