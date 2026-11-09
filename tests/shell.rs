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
fn the_window_saves_a_connection_lists_fields_and_edits_a_cell() {
    let server = Server::start(false);
    let file = server.dir.join("connections.json");
    std::env::set_var("KOVA_CONNECTIONS", &file);
    let mut ui = Ui::new();
    let saved: Value = serde_json::from_str(&ui.dispatch(&format!(
        r#"{{"op":"saveConnection","name":"KOVA","host":"127.0.0.1","port":{},"user":"","password":""}}"#,
        server.port
    )))
    .unwrap();
    assert!(saved["ok"].as_bool().unwrap_or(false), "{saved}");
    let id = saved["connection"]["id"].as_str().unwrap();
    let listed: Value = serde_json::from_str(&ui.dispatch(r#"{"op":"connections"}"#)).unwrap();
    assert!(
        listed["connections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["name"] == "KOVA"),
        "{listed}"
    );

    let connect: Value = serde_json::from_str(&ui.dispatch(&format!(
        r#"{{"op":"connect","id":"{id}","host":"127.0.0.1","port":{},"user":"","password":""}}"#,
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

    let collections: Value =
        serde_json::from_str(&ui.dispatch(r#"{"op":"collections","db":"shop"}"#)).unwrap();
    let orders = collections["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "orders")
        .unwrap();
    assert_eq!(orders["type"], "collection");

    let board: Value =
        serde_json::from_str(&ui.dispatch(r#"{"op":"dashboard","db":"shop"}"#)).unwrap();
    assert!(board["ok"].as_bool().unwrap_or(false), "{board}");
    let orders_row = board["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "orders")
        .unwrap();
    assert_eq!(orders_row["count"].as_i64(), Some(1), "{board}");
    assert!(orders_row["size"].as_i64().unwrap_or(0) > 0, "{board}");

    let profile: Value = serde_json::from_str(&ui.dispatch(
        r#"{"op":"profile","db":"shop","collection":"orders","filter":"{}"}"#,
    ))
    .unwrap();
    assert!(profile["ok"].as_bool().unwrap_or(false), "{profile}");
    assert_eq!(profile["documents"].as_i64(), Some(1), "{profile}");
    let qty = profile["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["name"] == "qty")
        .unwrap();
    assert_eq!(qty["fill"].as_f64(), Some(1.0), "{qty}");
    assert_eq!(qty["numeric"]["median"].as_f64(), Some(1.0), "{qty}");

    let status: Value = serde_json::from_str(&ui.dispatch(r#"{"op":"status"}"#)).unwrap();
    assert!(status["ok"].as_bool().unwrap_or(false), "{status}");
    assert!(status["memory"].as_i64().unwrap_or(-1) > 0, "{status}");
    assert!(status["disk"].as_i64().unwrap_or(-1) >= 0, "{status}");
    assert_eq!(status["feed"]["enabled"], false, "{status}");
    assert!(status.get("password").is_none(), "{status}");

    let fields: Value =
        serde_json::from_str(&ui.dispatch(r#"{"op":"fields","db":"shop","collection":"orders"}"#))
            .unwrap();
    let names = fields["fields"].as_array().unwrap();
    assert_eq!(names[0], Value::from("_id"));
    assert!(names.contains(&Value::from("sku")), "{fields}");
    assert!(names.contains(&Value::from("qty")), "{fields}");

    let indexes: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"indexes","db":"shop","collection":"orders"}"#),
    )
    .unwrap();
    assert!(indexes["ok"].as_bool().unwrap_or(false), "{indexes}");
    assert!(
        indexes["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["name"] == "_id_"
                    && item["key"] == "_id"
                    && item["parts"][0]["field"] == "_id"
                    && item["parts"][0]["order"] == 1
            }),
        "{indexes}"
    );

    let found: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    let row = &found["rows"][0];
    let qty = row["cells"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cell| cell["field"] == "qty")
        .unwrap();
    assert_eq!(qty["kind"], "int");
    assert_eq!(qty["text"], "1");
    let id_cell = row["cells"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cell| cell["field"] == "_id")
        .unwrap();
    assert_eq!(id_cell["kind"], "oid");
    let hex = id_cell["text"].as_str().unwrap();
    assert_eq!(hex.len(), 24, "{id_cell}");
    assert!(!hex.contains('$'), "{id_cell}");

    let page: Value = serde_json::from_str(&ui.dispatch(
        r#"{"op":"mql","db":"shop","mql":"db.getCollection(\"orders\").find({})","limit":1}"#,
    ))
    .unwrap();
    assert!(page["ok"].as_bool().unwrap_or(false), "{page}");
    assert_eq!(page["rows"].as_array().unwrap().len(), 1, "{page}");
    assert!(page["ms"].as_u64().is_some(), "{page}");
    let counted: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"count","db":"shop","mql":"db.orders.find({})"}"#),
    )
    .unwrap();
    assert!(counted["ok"].as_bool().unwrap_or(false), "{counted}");
    assert_eq!(counted["n"].as_i64(), Some(1), "{counted}");
    std::env::set_var("KOVA_DOWNLOADS", &server.dir);
    let exported: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"export","name":"orders","csv":"_id,qty\nabc,1\n"}"#),
    )
    .unwrap();
    assert!(exported["ok"].as_bool().unwrap_or(false), "{exported}");
    let csv_path = exported["path"].as_str().unwrap();
    assert!(
        std::fs::read_to_string(csv_path).unwrap().contains("qty"),
        "{exported}"
    );
    let more = shell(
        server.port,
        &[],
        "use shop\ndb.orders.insertOne({ sku: 8, qty: 4 })\n",
    );
    assert!(
        more.status.success(),
        "{}",
        String::from_utf8_lossy(&more.stderr)
    );
    let full = serde_json::json!({
        "op": "export",
        "db": "shop",
        "mql": "db.orders.find({})",
        "path": server.dir.join("orders-full.csv"),
    });
    let full: Value = serde_json::from_str(&ui.dispatch(&full.to_string())).unwrap();
    assert!(full["ok"].as_bool().unwrap_or(false), "{full}");
    assert_eq!(full["n"].as_u64(), Some(2), "{full}");
    let full_csv = std::fs::read_to_string(full["path"].as_str().unwrap()).unwrap();
    assert!(full_csv.lines().count() >= 3, "{full_csv}");
    assert!(full_csv.contains('4'), "{full_csv}");
    let filtered = serde_json::json!({
        "op": "export",
        "db": "shop",
        "mql": "db.orders.find({ sku: 8 })",
        "path": server.dir.join("orders-sku-8.csv"),
    });
    let filtered: Value = serde_json::from_str(&ui.dispatch(&filtered.to_string())).unwrap();
    assert_eq!(filtered["n"].as_u64(), Some(1), "{filtered}");
    let filtered_csv = std::fs::read_to_string(filtered["path"].as_str().unwrap()).unwrap();
    assert!(filtered_csv.contains('4'), "{filtered_csv}");
    assert_eq!(filtered_csv.lines().count(), 2, "{filtered_csv}");
    let view = serde_json::json!({
        "op": "createView",
        "db": "shop",
        "name": "open_orders",
        "mql": "db.orders.find({ sku: 9 })",
    });
    let view: Value = serde_json::from_str(&ui.dispatch(&view.to_string())).unwrap();
    assert!(view["ok"].as_bool().unwrap_or(false), "{view}");
    assert_eq!(view["viewOn"], "orders", "{view}");
    let listed: Value =
        serde_json::from_str(&ui.dispatch(r#"{"op":"collections","db":"shop"}"#)).unwrap();
    let saved_view = listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "open_orders")
        .unwrap();
    assert_eq!(saved_view["type"], "view", "{listed}");
    let seeded = shell(
        server.port,
        &[],
        "use shop\ndb.bonus.insertOne({ empno: 1, sal: 1600 })\ndb.emp.insertOne({ empno: 1, ename: \"ALLEN\" })\ndb.bonus.insertOne({ empno: 2, sal: 900 })\n",
    );
    assert!(
        seeded.status.success(),
        "{}",
        String::from_utf8_lossy(&seeded.stderr)
    );
    let joined = serde_json::json!({
        "op": "createView",
        "db": "shop",
        "name": "bonus_inner_join",
        "sql": "SELECT BONU._id, BONU.sal, EMPR.ename name FROM bonus BONU INNER JOIN emp EMPR ON BONU.empno = EMPR.empno",
    });
    let joined: Value = serde_json::from_str(&ui.dispatch(&joined.to_string())).unwrap();
    assert!(joined["ok"].as_bool().unwrap_or(false), "{joined}");
    assert_eq!(joined["viewOn"], "bonus", "{joined}");
    let joined_rows: Value = serde_json::from_str(&ui.dispatch(
        r#"{"op":"find","db":"shop","collection":"bonus_inner_join","filter":"{}"}"#,
    ))
    .unwrap();
    assert!(
        joined_rows["ok"].as_bool().unwrap_or(false),
        "{joined_rows}"
    );
    assert_eq!(joined_rows["rows"].as_array().unwrap().len(), 1, "{joined_rows}");
    let cells = joined_rows["rows"][0]["cells"].as_array().unwrap();
    let cell = |field: &str| {
        cells
            .iter()
            .find(|cell| cell["field"] == field)
            .unwrap_or_else(|| panic!("missing {field} in {joined_rows}"))
    };
    assert_eq!(cell("sal")["text"], "1600", "{joined_rows}");
    assert_eq!(cell("name")["text"], "ALLEN", "{joined_rows}");
    let as_mql = serde_json::json!({
        "op": "createView",
        "db": "shop",
        "name": "bonus_emp_view",
        "mql": "SELECT BONU._id, BONU.sal, EMPR.ename name FROM bonus BONU INNER JOIN emp EMPR ON BONU.empno = EMPR.empno",
    });
    let as_mql: Value = serde_json::from_str(&ui.dispatch(&as_mql.to_string())).unwrap();
    assert!(as_mql["ok"].as_bool().unwrap_or(false), "{as_mql}");
    assert_eq!(as_mql["viewOn"], "bonus", "{as_mql}");
    let refused = serde_json::json!({
        "op": "createView",
        "db": "shop",
        "name": "joined",
        "sql": "SELECT sal FROM bonus UNION SELECT sal FROM emp",
    });
    let refused: Value = serde_json::from_str(&ui.dispatch(&refused.to_string())).unwrap();
    assert!(!refused["ok"].as_bool().unwrap_or(true), "{refused}");
    let patch = format!(
        r#"{{"op":"patch","db":"shop","collection":"orders","id":{},"field":"qty","text":"3","kind":"int"}}"#,
        serde_json::to_string(row["id"].as_str().unwrap()).unwrap(),
    );
    let patched: Value = serde_json::from_str(&ui.dispatch(&patch)).unwrap();
    assert!(patched["ok"].as_bool().unwrap_or(false), "{patched}");
    let again: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    let qty = again["rows"][0]["cells"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cell| cell["field"] == "qty")
        .unwrap();
    assert_eq!(qty["text"], "3", "{again}");

    let refused = format!(
        r#"{{"op":"patch","db":"shop","collection":"orders","id":{},"field":"qty","text":"nope","kind":"int"}}"#,
        serde_json::to_string(row["id"].as_str().unwrap()).unwrap(),
    );
    let refused: Value = serde_json::from_str(&ui.dispatch(&refused)).unwrap();
    assert!(!refused["ok"].as_bool().unwrap_or(true), "{refused}");
    assert!(refused["keep"].as_bool().unwrap_or(false), "{refused}");
    let still: Value = serde_json::from_str(
        &ui.dispatch(r#"{"op":"find","db":"shop","collection":"orders","filter":"{ sku: 9 }"}"#),
    )
    .unwrap();
    let qty = still["rows"][0]["cells"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cell| cell["field"] == "qty")
        .unwrap();
    assert_eq!(qty["text"], "3", "{still}");

    let deleted: Value =
        serde_json::from_str(&ui.dispatch(&format!(r#"{{"op":"deleteConnection","id":"{id}"}}"#)))
            .unwrap();
    assert!(deleted["ok"].as_bool().unwrap_or(false), "{deleted}");
    let empty: Value = serde_json::from_str(&ui.dispatch(r#"{"op":"connections"}"#)).unwrap();
    assert!(
        !empty["connections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["name"] == "KOVA"),
        "{empty}"
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
