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

use crate::error::{GatewayError, Resource};
use fluss::error::{Error as FlussClientError, FlussError};

/// Classifies one native failure without exposing its detail.
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
            // Unclassified native failures use `backend`, not `internal`.
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
        // A schema is not a resource the API addresses on its own, so a missing one is reported
        // against the table it belongs to; that is the resource a caller can act on.
        FlussError::SchemaNotExist => GatewayError::not_found(format!(
            "the table schema does not exist while trying to {what}"
        ))
        .with_resource(Resource::Table),
        FlussError::PartitionNotExists => GatewayError::not_found(format!(
            "the partition does not exist while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        FlussError::PartitionAlreadyExists => GatewayError::already_exists(format!(
            "the partition already exists while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        // Asking for a partition of a table that declares no partition keys, or supplying a spec that
        // does not match the declared keys, is a malformed request rather than a missing resource.
        FlussError::TableNotPartitionedException | FlussError::PartitionSpecInvalidException => {
            GatewayError::invalid_argument(format!(
                "Fluss rejected the partition spec while trying to {what}"
            ))
            .with_resource(Resource::Partition)
        }
        // The table is at its configured partition limit. A caller can act on it, by dropping a
        // partition, which is what makes it a precondition rather than a gateway capacity failure.
        FlussError::PartitionMaxNumException => GatewayError::failed_precondition(format!(
            "the table holds the maximum number of partitions, {what} refused"
        ))
        .with_resource(Resource::Partition),
        FlussError::DatabaseNotEmpty => {
            GatewayError::failed_precondition(format!("the database is not empty, {what} refused"))
                .with_resource(Resource::Database)
        }
        FlussError::InvalidDatabaseException | FlussError::InvalidTableException => {
            GatewayError::invalid_argument(format!(
                "Fluss rejected the name while trying to {what}"
            ))
        }
        FlussError::InvalidConfigException
        | FlussError::InvalidAlterTableException
        | FlussError::InvalidReplicationFactor
        | FlussError::BucketMaxNumException => GatewayError::invalid_argument(format!(
            "Fluss rejected the definition while trying to {what}"
        )),
        FlussError::RequestTimeOut => {
            GatewayError::deadline_exceeded(format!("Fluss timed out while trying to {what}"))
        }
        FlussError::UnsupportedVersion => GatewayError::unsupported(format!(
            "Fluss does not support the request while trying to {what}"
        )),
        // TODO: Map caller authorization failures to 403 when fluss-rs supports user mode.
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

    pub(crate) fn api_failure(error: FlussError) -> FlussClientError {
        FlussClientError::FlussAPIError {
            api_error: fluss::error::ApiError {
                code: error.code(),
                message: "server detail".to_string(),
            },
        }
    }

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
                api_failure(FlussError::PartitionMaxNumException),
                ErrorKind::FailedPrecondition,
                "failed_precondition",
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
                api_failure(FlussError::InvalidConfigException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidAlterTableException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidReplicationFactor),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::RequestTimeOut),
                ErrorKind::DeadlineExceeded,
                "timeout",
            ),
            (
                api_failure(FlussError::UnsupportedVersion),
                ErrorKind::Unsupported,
                "unsupported",
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
