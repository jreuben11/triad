import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 50,
    duration: '60s',
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
    },
};

// Target: 10,000 events/s for 60s via the admin API or a direct PG insert endpoint.
// Requires a running Triad instance at TRIAD_BASE_URL.
const BASE_URL = __ENV.TRIAD_BASE_URL || 'http://localhost:8080';

export default function () {
    const payload = JSON.stringify({
        event_type: 'LoadTestEvent',
        payload: { ts: Date.now(), vu: __VU, iter: __ITER },
        kafka_topic: 'load-test',
    });

    const resp = http.post(`${BASE_URL}/outbox`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(resp, {
        'status is 2xx': (r) => r.status >= 200 && r.status < 300,
    });

    sleep(0.005);
}
