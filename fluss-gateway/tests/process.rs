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

//! End-to-end checks of the compiled binary: startup, health, SIGTERM draining, and exit codes.
//!
//! These spawn the real `fluss-gateway` executable (CARGO_BIN_EXE), so they exercise CLI parsing,
//! config loading, logging setup, and the production lifecycle exactly as an operator would.

use std::io::Write;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_fluss-gateway"))
}

/// Polls `url` until it answers 200 or the deadline passes.
fn await_http_ok(url: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if let Ok(response) = reqwest::blocking::get(url)
            && response.status() == 200
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn write_config(dir: &tempfile::TempDir, port: u16) -> std::path::PathBuf {
    let path = dir.path().join("gateway.yaml");
    let mut file = std::fs::File::create(&path).expect("config file");
    writeln!(file, "gateway.rest.listen: 127.0.0.1:{port}").expect("write");
    writeln!(file, "gateway.metrics.enabled: false").expect("write");
    path
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn send_sigterm(child: &Child) {
    // SAFETY: kill(2) with a live child pid owned by this test.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
}

/// Kills the child on drop so a failing assertion never leaks a running gateway
/// that could hold its port into later tests.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn an_invalid_configuration_fails_before_binding_with_exit_code_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.yaml");
    std::fs::write(&path, "gateway.unknown.key: true\n").expect("write");
    let output = binary().arg("--config").arg(&path).output().expect("run");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("gateway.unknown.key"),
        "stderr names the offending key: {stderr}"
    );
}

#[test]
fn the_binary_starts_serves_health_and_drains_on_sigterm_with_exit_code_0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let config = write_config(&dir, port);
    // The gateway inherits the test's stdout/stderr: piping without draining could fill the pipe
    // buffer and stall the child, and its few startup/drain log lines are useful on failure.
    let child = binary()
        .arg("--config")
        .arg(&config)
        .spawn()
        .expect("spawn");
    let mut guard = ChildGuard(child);
    let base = format!("http://127.0.0.1:{port}");
    assert!(
        await_http_ok(&format!("{base}/health"), Duration::from_secs(15)),
        "health"
    );
    send_sigterm(&guard.0);
    let start = Instant::now();
    let status = loop {
        if let Some(status) = guard.0.try_wait().expect("wait") {
            break status;
        }
        assert!(
            start.elapsed() < Duration::from_secs(35),
            "SIGTERM drain finished in time"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(status.code(), Some(0), "clean drain exits 0");
}

#[test]
fn a_bind_conflict_fails_serving_with_exit_code_1() {
    let holder = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = holder.local_addr().expect("addr").port();
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(&dir, port);
    let output = binary().arg("--config").arg(&config).output().expect("run");
    assert_eq!(output.status.code(), Some(1));
}
