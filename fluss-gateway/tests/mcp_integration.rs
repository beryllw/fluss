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

//! MCP integration tests: drive the Streamable HTTP frontend with the real rmcp
//! client (the way an agent would) against a fake `GatewayInstance`. No Fluss
//! cluster required.

mod harness;

use std::collections::HashMap;
use std::sync::Arc;

use fluss_gateway::auth::ConfigUserStoreAuthenticator;
use reqwest::header::{HeaderValue, AUTHORIZATION};
use rmcp::model::{object, CallToolRequestParams};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::ServiceExt;
use serde_json::Value;

use harness::{FakeInstance, McpTestServer};

/// `Authorization: Basic` value for `alice:secret`.
const ALICE_SECRET: &str = "Basic YWxpY2U6c2VjcmV0";
/// `Authorization: Basic` value for `alice:wrong`.
const ALICE_WRONG: &str = "Basic YWxpY2U6d3Jvbmc=";

/// Connect an rmcp client to the server with the given auth header, returning the
/// running client service (which has already completed the initialize handshake).
async fn connect(
    endpoint: String,
    auth_header: &str,
) -> Result<rmcp::service::RunningService<rmcp::RoleClient, ()>, Box<dyn std::error::Error>> {
    // The gateway authenticates `Authorization: Basic`. rmcp's `auth_header`
    // config forces a `Bearer` scheme, so set the header directly via custom
    // headers instead.
    let mut headers = HashMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(auth_header)?);
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(endpoint).custom_headers(headers),
    );
    let client = ().serve(transport).await?;
    Ok(client)
}

fn text_contents(result: &rmcp::model::CallToolResult) -> Vec<String> {
    result
        .content
        .iter()
        .filter_map(|content| content.raw.as_text().map(|text| text.text.clone()))
        .collect()
}

#[tokio::test]
async fn initialize_and_list_tools_exposes_four_readonly_tools() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    // The handshake negotiated server info / instructions.
    let info = client.peer_info().expect("server info after initialize");
    assert!(info.instructions.as_deref().unwrap_or("").contains("Fluss"));

    let tools = client.list_tools(Default::default()).await.unwrap();
    let mut names: Vec<String> = tools.tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["describe_table", "list_databases", "list_tables", "query"]
    );
    // Every tool advertises an input schema.
    for tool in &tools.tools {
        assert!(!tool.input_schema.is_empty(), "tool {} has no schema", tool.name);
    }

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn list_databases_tool_returns_fluss() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    let result = client
        .call_tool(CallToolRequestParams::new("list_databases"))
        .await
        .unwrap();

    let structured = result.structured_content.expect("structured content");
    assert_eq!(structured, serde_json::json!({ "databases": ["fluss"] }));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn describe_table_tool_returns_columns() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    let result = client
        .call_tool(CallToolRequestParams::new("describe_table").with_arguments(object(serde_json::json!({
                "database": "fluss",
                "table": "t",
            }))))
        .await
        .unwrap();

    let structured = result.structured_content.expect("structured content");
    assert_eq!(structured["database"], "fluss");
    assert_eq!(structured["table"], "t");
    let columns = structured["columns"].as_array().unwrap();
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[1]["name"], "name");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn query_tool_returns_rows() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    let result = client
        .call_tool(CallToolRequestParams::new("query").with_arguments(object(serde_json::json!({ "sql": "SELECT * FROM t" }))))
        .await
        .unwrap();

    let texts = text_contents(&result);
    let structured = result.structured_content.expect("structured content");
    assert_eq!(texts, vec!["SELECT * FROM t".to_string()]);
    assert_eq!(structured["row_count"], 2);
    assert_eq!(structured["truncated"], false);
    let rows = structured["rows"].as_array().unwrap();
    assert_eq!(rows[0], serde_json::json!({"id": 1, "name": "alice"}));
    assert_eq!(rows[1], serde_json::json!({"id": 2, "name": "bob"}));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn query_tool_truncates_at_max_rows() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    let result = client
        .call_tool(CallToolRequestParams::new("query").with_arguments(object(serde_json::json!({
                "sql": "SELECT * FROM t",
                "max_rows": 1,
            }))))
        .await
        .unwrap();

    let texts = text_contents(&result);
    let structured = result.structured_content.expect("structured content");
    assert_eq!(texts, vec!["SELECT * FROM t".to_string()]);
    assert_eq!(structured["row_count"], 1);
    assert_eq!(structured["truncated"], true);

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn query_tool_does_not_mark_exact_cap_as_truncated() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    let result = client
        .call_tool(CallToolRequestParams::new("query").with_arguments(object(serde_json::json!({
                "sql": "SELECT * FROM t",
                "max_rows": 2,
            }))))
        .await
        .unwrap();

    let texts = text_contents(&result);
    let structured = result.structured_content.expect("structured content");
    assert_eq!(texts, vec!["SELECT * FROM t".to_string()]);
    assert_eq!(structured["row_count"], 2);
    assert_eq!(structured["truncated"], false);

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn query_tool_rejects_non_readonly_sql() {
    let server = McpTestServer::start().await;
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();

    // The read-only guard rejects DDL before it reaches the SQL path; the tool
    // returns an error, surfaced to the client as a failed call.
    let outcome = client
        .call_tool(CallToolRequestParams::new("query").with_arguments(object(serde_json::json!({ "sql": "DROP TABLE t" }))))
        .await;
    assert!(outcome.is_err(), "DROP should be rejected, got {outcome:?}");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn rejects_unauthenticated_and_bad_credentials() {
    // A real authenticator (alice:secret) so credentials actually matter.
    let mut users = HashMap::new();
    users.insert("alice".to_string(), "secret".to_string());
    let authenticator = Arc::new(ConfigUserStoreAuthenticator::from_pairs(users).unwrap());
    let server =
        McpTestServer::start_with_authenticator(Arc::new(FakeInstance::new()), authenticator).await;

    // Wrong password: the 401 fails the initialize handshake.
    assert!(
        connect(server.endpoint(), ALICE_WRONG).await.is_err(),
        "bad credentials must fail to initialize"
    );

    // Correct password: handshake succeeds.
    let client = connect(server.endpoint(), ALICE_SECRET).await.unwrap();
    let dbs = client
        .call_tool(CallToolRequestParams::new("list_databases"))
        .await
        .unwrap();
    let _: Value = dbs.structured_content.expect("structured content");
    client.cancel().await.unwrap();
}
