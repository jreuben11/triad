import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 20,
    duration: '30s',
    thresholds: {
        http_req_duration: ['p(95)<2000'],
        http_req_failed: ['rate<0.01'],
    },
};

// Target: 1,000 sagas/s for 30s.
// Requires a running Triad instance at TRIAD_BASE_URL.
const BASE_URL = __ENV.TRIAD_BASE_URL || 'http://localhost:8080';

export default function () {
    const payload = JSON.stringify({
        saga_name: 'order-saga',
        steps: ['reserve-inventory', 'charge-payment', 'fulfill-order'],
        state: { order_id: `ord-${__VU}-${__ITER}`, amount: 100 },
    });

    const resp = http.post(`${BASE_URL}/sagas`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(resp, {
        'status is 2xx': (r) => r.status >= 200 && r.status < 300,
        'saga_id present': (r) => {
            try {
                return JSON.parse(r.body).saga_id !== undefined;
            } catch {
                return false;
            }
        },
    });

    sleep(0.02);
}
