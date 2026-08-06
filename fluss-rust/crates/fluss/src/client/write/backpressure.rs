// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cooperative KV backpressure: per-bucket write throttling from server pressure signals.
//!
//! Fluss piggybacks a per-bucket pressure value on every `PutKv` response: `0` (or absent) means
//! normal, and a value in `(0, 1]` is the normalized RocksDB L0 ratio of the DELAYED zone. The
//! sender records the signal here after each response, and delays the next request that would
//! touch a pressured bucket by `pressure * max_throttle` (default 3s, matching the Java client's
//! `client.writer.kv-backpressure.max-throttle`). Pressure therefore surfaces only as request
//! latency, counted against the write deadline, never as an error by itself.
//!
//! The throttle is deliberately conservative: a request carries every ready batch of one server
//! node, so delaying it slows co-located buckets too. That errs on the side of draining pressure,
//! which is the point of the cooperative protocol.

use crate::metadata::TableBucket;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Gauge: the most recent per-bucket pressure observed on PutKv responses (max over buckets).
pub const WRITER_KV_BACKPRESSURE_PRESSURE: &str = "fluss.client.writer.kv_backpressure.pressure";

/// Counter: accumulated scheduled throttle time in milliseconds.
pub const WRITER_KV_BACKPRESSURE_THROTTLE_MS_TOTAL: &str =
    "fluss.client.writer.kv_backpressure.throttle_ms.total";

/// One bucket's most recent pressure observation.
struct BucketPressure {
    pressure: f32,
    throttle_until: Instant,
}

/// Tracks per-bucket pressure and answers how long the next send touching a bucket must wait.
pub(crate) struct BackpressureThrottle {
    max_throttle: Duration,
    buckets: Mutex<HashMap<TableBucket, BucketPressure>>,
    pressure_gauge: metrics::Gauge,
    throttle_ms_total: metrics::Counter,
}

impl BackpressureThrottle {
    pub(crate) fn new(max_throttle: Duration) -> Self {
        Self {
            max_throttle,
            buckets: Mutex::new(HashMap::new()),
            pressure_gauge: metrics::gauge!(WRITER_KV_BACKPRESSURE_PRESSURE),
            throttle_ms_total: metrics::counter!(WRITER_KV_BACKPRESSURE_THROTTLE_MS_TOTAL),
        }
    }

    /// Records the pressure signal of one bucket response. Absent or non-positive pressure clears
    /// the bucket; a positive value schedules `pressure * max_throttle` of delay from now.
    pub(crate) fn observe(&self, bucket: &TableBucket, pressure: Option<f32>) {
        let mut buckets = self.buckets.lock();
        match pressure {
            Some(pressure) if pressure > 0.0 => {
                let pressure = pressure.min(1.0);
                let throttle = self.max_throttle.mul_f64(f64::from(pressure));
                self.throttle_ms_total
                    .increment(throttle.as_millis() as u64);
                buckets.insert(
                    bucket.clone(),
                    BucketPressure {
                        pressure,
                        throttle_until: Instant::now() + throttle,
                    },
                );
            }
            _ => {
                buckets.remove(bucket);
            }
        }
        let max_pressure = buckets
            .values()
            .map(|state| state.pressure)
            .fold(0.0f32, f32::max);
        self.pressure_gauge.set(f64::from(max_pressure));
    }

    /// The instant until which a send touching any of `buckets` must wait, if any. Expired
    /// entries are dropped on the way; the pressure value itself is only refreshed by responses.
    pub(crate) fn delay_until<'a>(
        &self,
        touched: impl IntoIterator<Item = &'a TableBucket>,
    ) -> Option<Instant> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        let mut latest: Option<Instant> = None;
        for bucket in touched {
            if let Some(state) = buckets.get(bucket) {
                if state.throttle_until <= now {
                    buckets.remove(bucket);
                } else {
                    latest = Some(latest.map_or(state.throttle_until, |current| {
                        current.max(state.throttle_until)
                    }));
                }
            }
        }
        latest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(id: i32) -> TableBucket {
        TableBucket::new(1, id)
    }

    #[test]
    fn positive_pressure_schedules_a_proportional_bounded_delay() {
        let throttle = BackpressureThrottle::new(Duration::from_secs(3));
        let before = Instant::now();

        throttle.observe(&bucket(0), Some(0.5));
        let until = throttle.delay_until([&bucket(0)]).expect("throttled");
        let delay = until - before;
        assert!(delay >= Duration::from_millis(1_400), "{delay:?}");
        assert!(delay <= Duration::from_millis(1_600), "{delay:?}");

        // Pressure above the normalized range is clamped to the maximum throttle.
        throttle.observe(&bucket(1), Some(7.0));
        let clamped = throttle.delay_until([&bucket(1)]).expect("throttled");
        assert!(clamped - before <= Duration::from_millis(3_100));
    }

    #[test]
    fn absent_or_zero_pressure_clears_the_bucket() {
        let throttle = BackpressureThrottle::new(Duration::from_secs(3));
        throttle.observe(&bucket(0), Some(0.9));
        assert!(throttle.delay_until([&bucket(0)]).is_some());

        throttle.observe(&bucket(0), Some(0.0));
        assert!(throttle.delay_until([&bucket(0)]).is_none());

        throttle.observe(&bucket(0), Some(0.9));
        throttle.observe(&bucket(0), None);
        assert!(throttle.delay_until([&bucket(0)]).is_none());
    }

    #[test]
    fn only_touched_buckets_contribute_and_the_latest_deadline_wins() {
        let throttle = BackpressureThrottle::new(Duration::from_secs(3));
        throttle.observe(&bucket(0), Some(0.1));
        throttle.observe(&bucket(1), Some(0.9));

        let low = throttle.delay_until([&bucket(0)]).expect("throttled");
        let both = throttle
            .delay_until([&bucket(0), &bucket(1)])
            .expect("throttled");
        assert!(both > low, "the most pressured bucket decides the delay");
        // An unrelated bucket is never delayed.
        assert!(throttle.delay_until([&bucket(2)]).is_none());
    }

    #[test]
    fn expired_throttles_are_dropped_on_read() {
        let throttle = BackpressureThrottle::new(Duration::from_millis(1));
        throttle.observe(&bucket(0), Some(1.0));
        std::thread::sleep(Duration::from_millis(5));
        assert!(throttle.delay_until([&bucket(0)]).is_none());
        // Dropped, not merely hidden: the map no longer holds the bucket.
        assert!(throttle.buckets.lock().is_empty());
    }
}
