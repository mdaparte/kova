//! The terminal client and the connection the window uses.
//!
//! `kova-shell` parses commands and prints documents. `kova-client` draws
//! the window and calls the same [`Client`]. Neither one adds a server command.

mod client;
mod dispatch;
mod edit;
mod parse;
mod window;

pub use client::{pair, Client};
pub use dispatch::Ui;
pub use edit::{complete_collection, read_loop, Recall};
pub use parse::{parse_bson, parse_command, parse_document, Command};

use std::sync::{Arc, Mutex};

use bson::{Bson, Document};

/// Oxide `#c2461b` on granite `#17191c` when the terminal can show color.
/// The same word, with no escapes, when it cannot.
pub fn banner(color: bool) -> String {
    if color {
        "\u{1b}[48;2;23;25;28m\u{1b}[38;2;194;70;27mKOVA\u{1b}[0m\n".to_string()
    } else {
        "KOVA\n".to_string()
    }
}

pub fn color_enabled(stdout_is_tty: bool) -> bool {
    if !stdout_is_tty {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var("TERM").ok().as_deref() == Some("dumb") {
        return false;
    }
    true
}

pub fn pretty(document: &Document) -> Result<String, String> {
    let value = Bson::Document(document.clone()).into_relaxed_extjson();
    serde_json::to_string_pretty(&value).map_err(|err| err.to_string())
}

pub fn pretty_documents(documents: &[Document]) -> Result<String, String> {
    let mut out = String::new();
    for document in documents {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&pretty(document)?);
        out.push('\n');
    }
    Ok(out)
}

/// Runs one command. `use` refreshes the collection names Tab completes.
pub fn run_line(
    client: &mut Client,
    database: &mut Option<String>,
    collections: &Arc<Mutex<Vec<String>>>,
    line: &str,
) -> Result<String, String> {
    if line.trim().is_empty() {
        return Ok(String::new());
    }
    match parse_command(line)? {
        Command::Help => Ok(command_help().to_string()),
        Command::ShowDbs => {
            let names = client.list_databases()?;
            Ok(names.join("\n"))
        }
        Command::Use { database: name } => {
            *database = Some(name.clone());
            let names = client.list_collections(&name).unwrap_or_default();
            *collections.lock().expect("collection names") = names;
            Ok(String::new())
        }
        Command::ShowCollections => {
            let name = database
                .as_deref()
                .ok_or_else(|| "no database selected".to_string())?;
            let names = client.list_collections(name)?;
            *collections.lock().expect("collection names") = names.clone();
            Ok(names.join("\n"))
        }
        Command::Find { collection, filter } => {
            let name = selected(database)?;
            let documents = client.find(name, &collection, filter)?;
            pretty_documents(&documents)
        }
        Command::InsertOne {
            collection,
            document,
        } => {
            let name = selected(database)?;
            let count = client.insert_one(name, &collection, document)?;
            let names = client.list_collections(name).unwrap_or_default();
            *collections.lock().expect("collection names") = names;
            Ok(format!("inserted {count}"))
        }
        Command::UpdateOne {
            collection,
            filter,
            update,
        } => {
            let name = selected(database)?;
            let count = client.update_one(name, &collection, filter, update)?;
            Ok(format!("updated {count}"))
        }
        Command::DeleteOne { collection, filter } => {
            let name = selected(database)?;
            let count = client.delete_one(name, &collection, filter)?;
            Ok(format!("deleted {count}"))
        }
        Command::Sql { statement } => {
            let name = selected(database)?;
            let documents = client.sql(name, &statement)?;
            pretty_documents(&documents)
        }
    }
}

pub fn command_help() -> &'static str {
    "\
show dbs
use shop
show collections
db.orders.find()
db.orders.find({ sku: 1 })
db.orders.insertOne({ sku: 9, qty: 1 })
db.orders.updateOne({ sku: 9 }, { $set: { qty: 2 } })
db.orders.deleteOne({ sku: 9 })
db.runCommand({ sql: \"select sku from orders\" })
help
"
}

fn selected(database: &Option<String>) -> Result<&str, String> {
    database
        .as_deref()
        .ok_or_else(|| "no database selected".to_string())
}

pub fn page() -> &'static str {
    window::PAGE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_lists_the_commands() {
        let help = command_help();
        assert!(help.contains("show dbs"));
        assert!(help.contains("db.orders.find()"));
        assert!(help.contains("help"));
    }

    #[test]
    fn banner_is_kova_and_color_uses_oxide_on_granite() {
        assert_eq!(banner(false), "KOVA\n");
        let colored = banner(true);
        assert!(colored.contains("KOVA"));
        assert!(colored.contains("38;2;194;70;27"));
        assert!(colored.contains("48;2;23;25;28"));
        assert!(!banner(false).contains('\u{1b}'));
    }

    #[test]
    fn tab_completes_a_collection_after_db_dot() {
        let names = vec!["orders".into(), "dept".into()];
        let (start, matches) = complete_collection("db.ord", 6, &names).unwrap();
        assert_eq!(start, 3);
        assert_eq!(matches, vec!["orders".to_string()]);
        assert!(complete_collection("show dbs", 8, &names).is_none());
    }

    #[test]
    fn up_arrow_recalls_the_previous_line() {
        let mut recall = Recall::new();
        recall.push("show dbs".into());
        recall.push("use shop".into());
        assert_eq!(recall.up("").as_deref(), Some("use shop"));
        assert_eq!(recall.up("use shop").as_deref(), Some("show dbs"));
        assert_eq!(recall.down("show dbs").as_deref(), Some("use shop"));
        assert_eq!(recall.down("use shop").as_deref(), Some(""));
    }

    #[test]
    fn the_window_page_uses_the_site_mark_and_colors() {
        let page = page();
        assert!(page.contains("#c2461b"));
        assert!(page.contains("#17191c"));
        assert!(page.contains("#eceeeb"));
        assert!(page.contains("Archivo"));
        assert!(page.contains("IBM Plex Sans"));
        assert!(page.contains("IBM Plex Mono"));
        assert!(page.contains("prefers-color-scheme: dark"));
        assert!(page.contains("width=\"4\" height=\"16\""));
    }
}
