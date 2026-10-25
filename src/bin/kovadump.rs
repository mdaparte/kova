//! Backup and restore a running Kova over the MongoDB port.
//!
//! ```text
//! kovadump backup  --db shop shop.kdmp
//! kovadump restore --db shop shop.kdmp
//! kovadump restore --db shop dump.sql
//! kovadump restore --db shop ./dump
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use kova::dump::{backup, restore, RestoreOptions, Target};

fn main() -> ExitCode {
    kova::install_panic_hook();
    match run(std::env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("kovadump: {err}");
            ExitCode::from(1)
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "\
kovadump backup  --db NAME [--host HOST] [--port PORT] [--user USER] [--password PASSWORD] FILE
kovadump restore --db NAME [--table NAME]... [--yes] [--host HOST] [--port PORT] [--user USER] [--password PASSWORD] PATH

backup writes a Kova dump of one database on a running server.
restore reads that file, a pg_dump plain SQL file, a mysqldump SQL file,
or a mongodump directory or archive, and inserts the rows.

--table restores only those collections or views. Repeat it, or separate
names with commas. Without --table, every collection in the dump is restored.
When a selected collection already exists, restore asks before replacing it.
--yes replaces it without asking. A non-interactive run must pass --yes
when something would be replaced.

HOST defaults to 127.0.0.1 and PORT to 27018. Leave user and password
empty when security is off. Set both when it is on."
        );
        if args.is_empty() {
            return Err("missing command".into());
        }
        return Ok(());
    }
    let command = args[0].as_str();
    if command != "backup" && command != "restore" {
        return Err(format!("unknown command {command}"));
    }
    let mut host = "127.0.0.1".to_string();
    let mut port = 27018u16;
    let mut database = None;
    let mut user = None;
    let mut password = None;
    let mut tables = Vec::new();
    let mut yes = false;
    let mut path = None;
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if let Some(value) = arg.strip_prefix("--db=") {
            database = Some(value.to_string());
        } else if arg == "--db" {
            database = Some(take(&args, &mut index, "--db")?);
        } else if let Some(value) = arg.strip_prefix("--host=") {
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
        } else if arg == "--yes" {
            yes = true;
        } else if let Some(value) = arg.strip_prefix("--table=") {
            push_tables(&mut tables, value)?;
        } else if arg == "--table" {
            push_tables(&mut tables, &take(&args, &mut index, "--table")?)?;
        } else if arg.starts_with('-') {
            return Err(format!("unknown option {arg}"));
        } else if path.is_some() {
            return Err(format!("unexpected argument {arg}"));
        } else {
            path = Some(PathBuf::from(arg));
        }
        index += 1;
    }
    let database = database.ok_or_else(|| "missing --db".to_string())?;
    let path = path.ok_or_else(|| "missing dump path".to_string())?;
    let target = Target {
        host,
        port,
        database: database.clone(),
        user,
        password,
    };
    if command == "backup" {
        if yes || !tables.is_empty() {
            return Err("--yes and --table apply to restore".into());
        }
        let report = backup(&target, &path)?;
        println!("wrote {report} to {}", path.display());
    } else {
        let report = restore(&target, &path, &RestoreOptions { tables, yes })?;
        println!("restored {report} into {database}");
    }
    Ok(())
}

fn push_tables(tables: &mut Vec<String>, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err("missing table name".into());
    }
    for name in value.split(',') {
        let name = name.trim();
        if name.is_empty() {
            return Err("empty table name".into());
        }
        tables.push(name.to_string());
    }
    Ok(())
}

fn take(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_port(text: &str) -> Result<u16, String> {
    text.parse::<u16>()
        .map_err(|_| format!("port {text} is not a number"))
}
