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

//! REST-specific content negotiation and bounded-capacity helpers.

use crate::error::{ErrorKind, GatewayError};
use axum::http::{HeaderMap, header};

/// Checks that a caller accepts the JSON representation returned by an endpoint.
pub fn ensure_json_acceptable(headers: &HeaderMap) -> Result<(), GatewayError> {
    if headers.get(header::ACCEPT).is_none() {
        return Ok(());
    }
    let mut json_preference: Option<(u8, u16)> = None;
    for value in headers.get_all(header::ACCEPT) {
        let value = value
            .to_str()
            .map_err(|_| GatewayError::new(ErrorKind::NotAcceptable, "unreadable Accept header"))?;
        for range in value.split(',') {
            let (media, quality) = parse_media_range(range)?;
            let specificity = match media.as_str() {
                "application/json" => Some(2),
                "application/*" => Some(1),
                "*/*" => Some(0),
                _ => None,
            };
            if let Some(specificity) = specificity {
                json_preference = match json_preference {
                    Some((best_specificity, best_quality)) if best_specificity > specificity => {
                        Some((best_specificity, best_quality))
                    }
                    Some((best_specificity, best_quality)) if best_specificity == specificity => {
                        Some((best_specificity, best_quality.max(quality)))
                    }
                    _ => Some((specificity, quality)),
                };
            }
        }
    }
    if json_preference.is_some_and(|(_, quality)| quality > 0) {
        Ok(())
    } else {
        Err(GatewayError::new(
            ErrorKind::NotAcceptable,
            "response is application/json, which the request does not accept",
        ))
    }
}

fn parse_media_range(range: &str) -> Result<(String, u16), GatewayError> {
    let mut parts = range.split(';');
    let media = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
    if media.is_empty() {
        return Err(not_acceptable("empty media range in Accept header"));
    }
    let mut quality = 1000;
    let mut saw_quality = false;
    for parameter in parts {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            return Err(not_acceptable("malformed Accept parameter"));
        };
        if name.trim().eq_ignore_ascii_case("q") {
            if saw_quality {
                return Err(not_acceptable("duplicate Accept quality"));
            }
            quality = parse_quality(value.trim())?;
            saw_quality = true;
        }
    }
    Ok((media, quality))
}

fn parse_quality(value: &str) -> Result<u16, GatewayError> {
    let quality: f32 = value
        .parse()
        .map_err(|_| not_acceptable("Accept quality must be between 0 and 1"))?;
    if !quality.is_finite() || !(0.0..=1.0).contains(&quality) {
        return Err(not_acceptable("Accept quality must be between 0 and 1"));
    }
    Ok((quality * 1000.0).round() as u16)
}

fn not_acceptable(message: &str) -> GatewayError {
    GatewayError::new(ErrorKind::NotAcceptable, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn accepts_json_and_wildcards() {
        for value in [
            "application/json",
            "application/*",
            "*/*",
            "text/plain;q=0.5, application/json;q=0.1",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_str(value).unwrap());
            assert!(ensure_json_acceptable(&headers).is_ok(), "rejected {value}");
        }
    }

    #[test]
    fn rejects_excluded_or_malformed_json_ranges() {
        for value in [
            "text/plain",
            "application/json;q=0",
            "application/json;q=0, */*;q=1",
            "application/json;q=0, application/*;q=0.5",
            "application/json;q=2",
            "application/json;q=oops",
            "application/json;q=1;q=1",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_str(value).unwrap());
            assert!(
                ensure_json_acceptable(&headers).is_err(),
                "accepted {value}"
            );
        }
    }
}
