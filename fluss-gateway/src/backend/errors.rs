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

//! Classification of native `fluss-rs` failures.
//!
//! Its own module because both layers above it need it: the pool classifies a failed dial, and the
//! backend classifies a failed operation. Nothing outside this module inspects a `fluss` error type.

use crate::error::{GatewayError, Resource};
use fluss::error::{Error as FlussClientError, FlussError};

/// Classifies one native failure, with `what` naming the attempted operation in the client-facing
/// message.
///
/// The rules the gateway commits to: an error code Fluss defines for a resource keeps that meaning,
/// service authentication or authorization failures are backend deployment faults, a retriable
/// transport failure is a transient outage, and anything the client cannot classify is reported as a
/// backend failure rather than a gateway-internal one. Messages carry the operation, never the native
/// error text, which can contain addresses and payload detail; that goes to the log.
pub(crate) fn map_fluss_error(what: &str, error: FlussClientError) -> GatewayError {
    if let Some(api_error) = error.api_error()
        && let Some(mapped) = map_api_error(what, api_error)
    {
        return mapped;
    }
    match &error {
        FlussClientError::UnsupportedOperation { .. }
        | FlussClientError::UnsupportedVersion { .. } => {
            log::warn!("Fluss does not support the request while trying to {what}: {error}");
            GatewayError::unsupported(format!(
                "Fluss does not support the request while trying to {what}"
            ))
        }
        FlussClientError::IllegalArgument { .. } => GatewayError::invalid_argument(format!(
            "Fluss rejected the request while trying to {what}"
        )),
        _ if error.is_retriable() => {
            log::warn!("Fluss is temporarily unavailable while trying to {what}: {error}");
            GatewayError::unavailable(format!("Fluss is unavailable while trying to {what}"))
        }
        _ => {
            log::error!("the Fluss request failed while trying to {what}: {error}");
            // An unclassifiable native failure is the backend's, not the gateway's (FIP-49 `backend`).
            GatewayError::backend(format!("Fluss failed while trying to {what}"))
        }
    }
}

/// Maps the protocol error codes that carry a meaning of their own. `None` falls through to the
/// transport-level classification.
fn map_api_error(what: &str, api_error: FlussError) -> Option<GatewayError> {
    Some(match api_error {
        FlussError::DatabaseNotExist => GatewayError::not_found(format!(
            "the database does not exist while trying to {what}"
        ))
        .with_resource(Resource::Database),
        FlussError::TableNotExist => {
            GatewayError::not_found(format!("the table does not exist while trying to {what}"))
                .with_resource(Resource::Table)
        }
        FlussError::DatabaseAlreadyExist => GatewayError::already_exists(format!(
            "the database already exists while trying to {what}"
        ))
        .with_resource(Resource::Database),
        FlussError::TableAlreadyExist => {
            GatewayError::already_exists(format!("the table already exists while trying to {what}"))
                .with_resource(Resource::Table)
        }
        FlussError::DatabaseNotEmpty => {
            GatewayError::failed_precondition(format!("the database is not empty, {what} refused"))
                .with_resource(Resource::Database)
        }
        FlussError::InvalidDatabaseException | FlussError::InvalidTableException => {
            GatewayError::invalid_argument(format!(
                "Fluss rejected the name while trying to {what}"
            ))
        }
        // In service mode, both failures are deployment faults, including protocol mismatches.
        // User mode may later map a caller-specific authorization failure to 403.
        FlussError::AuthenticateException => {
            log::error!("Fluss rejected the gateway connection while trying to {what}");
            GatewayError::backend(format!("Fluss rejected the gateway while trying to {what}"))
        }
        FlussError::AuthorizationException => {
            log::error!("Fluss denied the gateway while trying to {what}");
            GatewayError::backend(format!("Fluss denied the gateway while trying to {what}"))
        }
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::error::ErrorKind;

    /// A native failure carrying one Fluss protocol error code.
    pub(crate) fn api_failure(error: FlussError) -> FlussClientError {
        FlussClientError::FlussAPIError {
            api_error: fluss::error::ApiError {
                code: error.code(),
                message: "server detail".to_string(),
            },
        }
    }

    /// Every mapped condition keeps the class and the wire code a caller can act on, and no message
    /// quotes the native error text.
    #[test]
    fn native_failures_map_to_their_gateway_class_and_code() {
        let cases = [
            (
                api_failure(FlussError::DatabaseNotExist),
                ErrorKind::NotFound,
                "database_not_found",
            ),
            (
                api_failure(FlussError::TableNotExist),
                ErrorKind::NotFound,
                "table_not_found",
            ),
            (
                api_failure(FlussError::DatabaseAlreadyExist),
                ErrorKind::AlreadyExists,
                "database_already_exists",
            ),
            (
                api_failure(FlussError::TableAlreadyExist),
                ErrorKind::AlreadyExists,
                "table_already_exists",
            ),
            (
                api_failure(FlussError::DatabaseNotEmpty),
                ErrorKind::FailedPrecondition,
                "database_not_empty",
            ),
            (
                api_failure(FlussError::InvalidDatabaseException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::AuthenticateException),
                ErrorKind::Backend,
                "backend",
            ),
            (
                api_failure(FlussError::AuthorizationException),
                ErrorKind::Backend,
                "backend",
            ),
            (
                FlussClientError::UnsupportedVersion {
                    message: "server detail".to_string(),
                },
                ErrorKind::Unsupported,
                "unsupported",
            ),
            (
                FlussClientError::IllegalArgument {
                    message: "server detail".to_string(),
                },
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::NetworkException),
                ErrorKind::Unavailable,
                "unavailable",
            ),
            (
                api_failure(FlussError::NotLeaderOrFollower),
                ErrorKind::Unavailable,
                "unavailable",
            ),
            (
                FlussClientError::RowConvertError {
                    message: "server detail".to_string(),
                },
                ErrorKind::Backend,
                "backend",
            ),
        ];
        for (native, expected_kind, expected_code) in cases {
            let rendered = native.to_string();
            let mapped = map_fluss_error("list the databases", native);
            assert_eq!(mapped.kind(), expected_kind, "{rendered}");
            assert_eq!(mapped.code(), expected_code, "{rendered}");
            assert!(
                mapped.message().contains("list the databases"),
                "{}",
                mapped.message()
            );
            assert!(
                !mapped.message().contains("server detail"),
                "the native detail must stay in the log: {}",
                mapped.message()
            );
        }
    }
}
