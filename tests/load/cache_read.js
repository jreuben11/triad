import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 100,
    duration: '60s',
    thresholds: {
        http_req_duration: ['p(95)<50'],
        http_req_failed: ['rate<0.01'],
        // Cache hit rate > 95% (tracked via Prometheus, asserted by assert.rs).
    },
};

// Target: 5,000 reads/s for 60s with >95% cache hit rate.
// Requires a running Triad instance at TRIAD_BASE_URL.
const BASE_URL = __ENV.TRIAD_BASE_URL || 'http://localhost:8080';
const FEATURE_NAMES = ['dark-mode', 'new-checkout', 'beta-ui', 'gradual-rollout'];

export default function () {
    const flag = FEATURE_NAMES[__ITER % FEATURE_NAMES.length];
    const resp = http.get(`${BASE_URL}/flags/${flag}`);

    check(resp, {
        'status is 200': (r) => r.status === 200,
        'has enabled field': (r) => {
            try {
                return JSON.parse(r.body).enabled !== undefined;
            } catch {
                return false;
            }
        },
    });

    sleep(0.0002);
}
