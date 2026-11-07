//! Terminal client for a running Kova.
//!
//! ```text
//! kova-shell
//! kova-shell --host 127.0.0.1 --port 27018
//! ```
//!
//! Lines are commands. A line is not JavaScript.

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use kova::shell::{self, Client};

fn main() -> ExitCode {
    kova::install_panic_hook();
    match run(std::env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("kova-shell: {err}");
            ExitCode::from(1)
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "\
kova-shell [--host HOST] [--port PORT] [--user USER] [--password PASSWORD]

HOST defaults to 127.0.0.1 and PORT to 27018. Leave user and password
empty when security is off. Set both when it is on.

show dbs
use shop
show collections
db.orders.find({{ sku: 1 }})
db.orders.insertOne({{ sku: 9, qty: 1 }})
db.orders.updateOne({{ sku: 9 }}, {{ $set: {{ qty: 2 }} }})
db.orders.deleteOne({{ sku: 9 }})
db.runCommand({{ sql: \"select sku from orders\" }})"
        );
        return Ok(());
    }
    let mut host = "127.0.0.1".to_string();
    let mut port = 27018u16;
    let mut user = None;
    let mut password = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if let Some(value) = arg.strip_prefix("--host=") {
            host = value.to_string();
        } else if arg == "--host" {
            host = take(&args, &mut index, "--host")?;
        } else if let Some(value) = arg.strip_prefix("--port=") {
            port = parse_port(value)?;
        } else if arg == "--port" {
            port = parse_port(&take(&args, &mut index, "--port")?)?;
        } else if let Some(value) = arg.strip_prefix("--user=") {
            user = Some(value.to_string());
        } else if arg == "--user" {
            user = Some(take(&args, &mut index, "--user")?);
        } else if let Some(value) = arg.strip_prefix("--password=") {
            password = Some(value.to_string());
        } else if arg == "--password" {
            password = Some(take(&args, &mut index, "--password")?);
        } else {
            return Err(format!("unknown argument {arg}"));
        }
        index += 1;
    }
    let mut client = Client::connect(&host, port, user.as_deref(), password.as_deref())?;
    print!(
        "{}",
        shell::banner(shell::color_enabled(io::stdout().is_terminal()))
    );
    let _ = io::stdout().flush();
    let database = Arc::new(Mutex::new(None::<String>));
    let collections = Arc::new(Mutex::new(Vec::new()));
    let prompt_db = Arc::clone(&database);
    let line_db = Arc::clone(&database);
    let line_collections = Arc::clone(&collections);
    shell::read_loop(
        move || match prompt_db.lock().expect("database").as_deref() {
            Some(name) => format!("{name}> "),
            None => "> ".to_string(),
        },
        Arc::clone(&collections),
        move |line| {
            let mut current = line_db.lock().expect("database");
            match shell::run_line(&mut client, &mut *current, &line_collections, &line) {
                Ok(text) => {
                    if !text.is_empty() {
                        println!("{text}");
                        let _ = io::stdout().flush();
                    }
                }
                Err(err) => eprintln!("kova-shell: {err}"),
            }
        },
    )
}

fn take(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

fn parse_port(text: &str) -> Result<u16, String> {
    text.parse::<u16>()
        .map_err(|_| format!("port {text} is out of range"))
}
