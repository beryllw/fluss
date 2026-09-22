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

//! Native execution of request-scoped point and prefix lookup batches.

use crate::backend::context::RequestContext;
use crate::backend::errors::map_fluss_error;
use crate::backend::{
    LookupOutcome, LookupOutcomeKind, LookupRequest, PrefixLookupOutcome, PrefixLookupOutcomeKind,
    PrefixLookupRequest,
};
use crate::error::{GatewayError, GatewayResult};
use arrow::array::RecordBatch;
use fluss::client::{FlussConnection, FlussTable, LookupResult};
use fluss::error::Error as FlussClientError;
use fluss::metadata::RowType;
use fluss::record::RowAppendRecordBatchBuilder;
use fluss::row::InternalRow;
use futures_util::StreamExt;
use futures_util::stream;
use std::sync::Arc;

/// Bounds the native calls launched by one admitted HTTP request.
const MAX_CONCURRENT_LOOKUPS_PER_REQUEST: usize = 32;

pub(crate) async fn lookup(
    connection: &Arc<FlussConnection>,
    ctx: &RequestContext,
    request: LookupRequest,
) -> GatewayResult<Vec<LookupOutcome>> {
    let (table_info, keys) = request.into_parts();
    let table = FlussTable::new(connection.as_ref(), connection.get_metadata(), table_info);

    // Build every lookuper before starting an RPC. Construction validates the table and key
    // encoder, so a request-level setup failure cannot leave a partially executed batch.
    let mut calls = Vec::with_capacity(keys.len());
    for (input_index, key) in keys.into_iter().enumerate() {
        let mut lookuper = table
            .new_lookup()
            .and_then(|lookup| lookup.create_lookuper())
            .map_err(|error| map_prepare_error("prepare the primary-key lookup", error, ctx))?;
        calls.push(async move {
            LookupOutcome {
                input_index,
                kind: point_outcome(lookuper.lookup(&key).await, ctx),
            }
        });
    }

    Ok(stream::iter(calls)
        .buffered(MAX_CONCURRENT_LOOKUPS_PER_REQUEST)
        .collect()
        .await)
}

pub(crate) async fn prefix_lookup(
    connection: &Arc<FlussConnection>,
    ctx: &RequestContext,
    request: PrefixLookupRequest,
) -> GatewayResult<Vec<PrefixLookupOutcome>> {
    let (table_info, prefix_columns, prefixes, max_rows_per_prefix) = request.into_parts();
    let row_type = table_info.row_type().clone();
    let table = FlussTable::new(connection.as_ref(), connection.get_metadata(), table_info);

    // Build all lookupers before RPCs so prefix validation remains request-level.
    let mut calls = Vec::with_capacity(prefixes.len());
    for (input_index, prefix) in prefixes.into_iter().enumerate() {
        let mut lookuper = table
            .new_lookup()
            .and_then(|lookup| lookup.lookup_by(prefix_columns.clone()).create_lookuper())
            .map_err(|error| map_prepare_error("prepare the prefix lookup", error, ctx))?;
        let row_type = &row_type;
        calls.push(async move {
            PrefixLookupOutcome {
                input_index,
                kind: prefix_outcome(
                    lookuper.lookup(&prefix).await,
                    row_type,
                    max_rows_per_prefix,
                    ctx,
                ),
            }
        });
    }

    Ok(stream::iter(calls)
        .buffered(MAX_CONCURRENT_LOOKUPS_PER_REQUEST)
        .collect()
        .await)
}

fn point_outcome(
    result: Result<LookupResult, FlussClientError>,
    ctx: &RequestContext,
) -> LookupOutcomeKind {
    match result {
        Ok(result) => match result.to_record_batch() {
            Ok(batch) if batch.num_rows() == 0 => LookupOutcomeKind::NotFound,
            Ok(batch) if batch.num_rows() == 1 => LookupOutcomeKind::Found(batch),
            Ok(_) => LookupOutcomeKind::Error(GatewayError::internal(
                "Fluss returned more than one row for a primary-key lookup",
            )),
            Err(error) => LookupOutcomeKind::Error(map_fluss_error(
                "decode a primary-key lookup result",
                error,
                Some(ctx),
            )),
        },
        Err(error) => {
            LookupOutcomeKind::Error(map_fluss_error("look up a primary key", error, Some(ctx)))
        }
    }
}

fn prefix_outcome(
    result: Result<LookupResult, FlussClientError>,
    row_type: &RowType,
    max_rows: usize,
    ctx: &RequestContext,
) -> PrefixLookupOutcomeKind {
    match result {
        Ok(result) => match result
            .get_rows()
            .and_then(|rows| bounded_prefix_rows(&rows, row_type, max_rows))
        {
            Ok((batch, truncated)) => PrefixLookupOutcomeKind::Rows { batch, truncated },
            Err(error) => PrefixLookupOutcomeKind::Error(map_fluss_error(
                "decode a prefix lookup result",
                error,
                Some(ctx),
            )),
        },
        Err(error) => PrefixLookupOutcomeKind::Error(map_fluss_error(
            "look up a key prefix",
            error,
            Some(ctx),
        )),
    }
}

// The RPC currently fetches all matches. Materialize only the admitted rows as Arrow and
// release the native result before buffering outcomes; slicing a full batch retains its buffers.
fn bounded_prefix_rows<T: InternalRow>(
    rows: &[T],
    row_type: &RowType,
    max_rows: usize,
) -> Result<(RecordBatch, bool), FlussClientError> {
    let mut builder = RowAppendRecordBatchBuilder::new(row_type)?;
    for row in rows.iter().take(max_rows) {
        builder.append(row)?;
    }
    Ok((
        Arc::unwrap_or_clone(builder.build_arrow_record_batch()?),
        rows.len() > max_rows,
    ))
}

/// Preserves client validation details while classifying other setup failures normally.
fn map_prepare_error(what: &str, error: FlussClientError, ctx: &RequestContext) -> GatewayError {
    match error {
        FlussClientError::IllegalArgument { message } => GatewayError::invalid_argument(message),
        other => map_fluss_error(what, other, Some(ctx)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, Schema};
    use fluss::row::GenericRow;

    #[test]
    fn truncation_is_deterministic_at_the_boundary() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .build()
            .unwrap();
        let rows: Vec<_> = (0..5)
            .map(|id| {
                let mut row = GenericRow::new(1);
                row.set_field(0, id);
                row
            })
            .collect();
        for (limit, expected_rows, expected_truncated) in [(5, 5, false), (2, 2, true)] {
            let (batch, truncated) = bounded_prefix_rows(&rows, schema.row_type(), limit).unwrap();
            assert_eq!(batch.num_rows(), expected_rows);
            assert_eq!(truncated, expected_truncated);
        }
        let (batch, truncated) = bounded_prefix_rows(&rows[..0], schema.row_type(), 2).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(!truncated);
    }
}
