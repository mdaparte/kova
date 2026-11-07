//! The shell and the window talk to a running Kova.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kova::shell::Ui;
use serde_json::Value;

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Server {
    fn start(security: bool) -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = std::env::temp_dir().join(format!(
            "kova-shell-{}-{}-{}",
            std::process::id(),
            port,
            security as u8
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("kova.toml");
        std::fs::write(
            &config,
            format!(
                r#"
port = {port}
log_level = "error"
warn_on_non_mongo_queries = false

[storage]
data_dir = "{data}"
wal_fsync = false
buffer_pool_pages = 64

[security]
enabled = {security}

[feed]
enabled = false

[log]
target = "stderr"
rotate = "none"
"#,
                data = dir.join("data").display(),
                security = if security { "true" } else { "false" },
            ),
        )
        .unwrap();
        let child = Command::new(bin("kova"))
            .env("KOVA_CONFIG", &config)
            .env("KOVA_PORT", port.to_string())
            .env("KOVA_LOG_LEVEL", "error")
            .env(
                "KOVA_SECURITY_ENABLED",
                if security { "true" } else { "false" },
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn kova");
        let mut server = Self { child, port, dir };
        server.wait_until_listening();
        server
    }

    fn wait_until_listening(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().ok().flatten() {
                let mut err = String::new();
                if let Some(mut stderr) = self.child.stderr.take() {
                    let _ = stderr.read_to_string(&mut err);
                }
                panic!("kova exited early: {status}\n{err}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("kova did not listen on {}", self.port);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn bin(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("test executable");
    let mut dir = exe.parent().unwrap_or_else(|| Path::new("."));
    if dir.ends_with("deps") {
        dir = dir.parent().unwrap_or(dir);
    }
    let path = dir.join(name);
    assert!(path.is_file(), "missing {}", path.display());
    path
}

fn shell(port: u16, args: &[&str], script: &str) -> std::process::Output {
    let mut command = Command::new(bin("kova-shell"));
    command
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn kova-shell");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    child.wait_with_output().expect("kova-shell")
}

#[test]
fn shell_lists_finds_updates_and_runs_sql() {
    let server = Server::start(false);
    let output = shell(
        server.port,
        &[],
        "\
use shop
db.orders.insertOne({ sku: 9, qty: 1 })
show dbs
show collections
db.orders.find({ sku: 9 })
db.orders.updateOne({ sku: 9 }, { $set: { qty: 2 } })
db.orders.find({ sku: 9 })
db.runCommand({ sql: \"select sku from orders\" })
",
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.starts_with("KOVA\n"), "{stdout}");
    assert!(!stdout.contains('\u{1b}'), "a pipe has no color: {stdout}");
    assert!(stdout.contains("shop"), "{stdout}");
    assert!(stdout.contains("orders"), "{stdout}");
    assert!(
        stdout.contains("\"qty\": 1") || stdout.contains("\"qty\":1"),
        "{stdout}"
    );
    assert!(
        stdout.contains("\"qty\": 2") || stdout.contains("\"qty\":2"),
        "{stdout}"
    );
    assert!(
        stdout.contains("\"sku\": 9") || stdout.contains("\"sku\":9"),
        "{stdout}"
    );
}

#[test]
fn a_wrong_password_is_refused() {
    let server = Server::start(true);
    let output = shell(
        server.port,
        &["--user", "nobody", "--password", "wrong"],
        "",
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("authentication failed"), "{stderr}");
}

#[test]
fn the_window_edits_a_document_and_keeps_a_refused_one() {
    let server = Server::start(false);
    let mut ui = Ui::new();
    let connect: Value = serde_json::from_str(&ui.dispatch(&format!(
        r#"{{"op":"connect","host":"127.0.0.1","port":{},"user":"","password":""}}"#,
        server.port
    )))
    .unwrap();
    assert!(connect["ok"].as_bool().unwrap_or(false), "{connect}");

    let seeded = shell(
        server.port,
        &[],
        "use shop\ndb.orders.insertOne({ sku: 9, qty: 1 })\n",
    );
    assert!(
        seeded.status.success(),
        "{}",
        String::from_utf8_lossy(&seeded.stderr)
    );

    let databases: Value = serde_json::from_str(&ui.dispatch(r#"{"op":"databases"}"#)).unwrap();
    let names = databases["names"].as_array().unwrap();
    assert!(names.contains(&Value::from("shop")), "{databases}");

    let collections: Value =
        serde_json::from_str(&ui.dispatch(r#"{"op":"collections","db":"shop"}"#)).unwrap();
    assert!(
        collections["names"]
            .as_array()
            .unwrap()
            .contains(&Value::from("orders")),
        "{collections}"
    );

    let found: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    assert!(found["ok"].as_bool().unwrap_or(false), "{found}");
    let row = &found["rows"][0];
    let text = row["text"]
        .as_str()
        .unwrap()
        .replace("\"qty\": 1", "\"qty\": 2");
    let save = format!(
        r#"{{"op":"save","db":"shop","collection":"orders","id":{},"document":{}}}"#,
        serde_json::to_string(row["id"].as_str().unwrap()).unwrap(),
        serde_json::to_string(&text).unwrap(),
    );
    let saved: Value = serde_json::from_str(&ui.dispatch(&save)).unwrap();
    assert!(saved["ok"].as_bool().unwrap_or(false), "{saved}");

    let again: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    assert!(
        again["rows"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"qty\": 2"),
        "{again}"
    );

    let refused = format!(
        r#"{{"op":"save","db":"shop","collection":"orders","id":{},"document":"{{\"$nope\":1}}"}}"#,
        serde_json::to_string(row["id"].as_str().unwrap()).unwrap(),
    );
    let refused: Value = serde_json::from_str(&ui.dispatch(&refused)).unwrap();
    assert!(!refused["ok"].as_bool().unwrap_or(true), "{refused}");
    assert!(refused["keep"].as_bool().unwrap_or(false), "{refused}");
    let still: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    assert!(
        still["rows"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"qty\": 2"),
        "{still}"
    );
}

#[test]
fn one_of_user_and_password_is_refused() {
    let err = kova::shell::pair(Some("ada"), None).unwrap_err();
    assert!(err.contains("both"));
}

#[test]
fn a_wrong_password_on_the_window_is_refused() {
    let server = Server::start(true);
    let mut ui = Ui::new();
    let answer: Value = serde_json::from_str(&ui.dispatch(&format!(
        r#"{{"op":"connect","host":"127.0.0.1","port":{},"user":"nobody","password":"wrong"}}"#,
        server.port
    )))
    .unwrap();
    assert!(!answer["ok"].as_bool().unwrap_or(true), "{answer}");
    assert!(
        answer["error"]
            .as_str()
            .unwrap()
            .contains("authentication failed"),
        "{answer}"
    );
}

#[test]
fn without_a_display_the_client_names_the_shell() {
    let output = Command::new(bin("kova-client"))
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .output()
        .expect("spawn kova-client");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("kova-shell"), "{stderr}");
}
