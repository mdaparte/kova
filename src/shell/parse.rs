//! Command lines and the relaxed documents those lines carry.
//!
//! A line is a command. It is not JavaScript. `{ sku: 1 }` is accepted
//! because that is how the commands are written; quoted keys are accepted
//! too, so a document pretty-printed as JSON parses again.

use bson::{oid::ObjectId, Bson, Document};

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Help,
    ShowDbs,
    Use {
        database: String,
    },
    ShowCollections,
    Find {
        collection: String,
        filter: Document,
    },
    InsertOne {
        collection: String,
        document: Document,
    },
    UpdateOne {
        collection: String,
        filter: Document,
        update: Document,
    },
    DeleteOne {
        collection: String,
        filter: Document,
    },
    Sql {
        statement: String,
    },
}

pub fn parse_command(line: &str) -> Result<Command, String> {
    let trimmed = line.trim();
    let line = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    let unknown = || format!("unknown command: {line}");
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["?"] | ["help"] => return Ok(Command::Help),
        ["show", "dbs"] => return Ok(Command::ShowDbs),
        ["show", "collections"] => return Ok(Command::ShowCollections),
        ["use", name] if is_name(name) => {
            return Ok(Command::Use {
                database: (*name).to_string(),
            });
        }
        _ => {}
    }
    let Some(rest) = line.strip_prefix("db.") else {
        return Err(unknown());
    };
    if let Some(after) = rest.strip_prefix("runCommand") {
        let args = call_args(after).ok_or_else(unknown)?;
        if args.len() != 1 {
            return Err(unknown());
        }
        let doc = parse_document(&args[0])?;
        let Some(statement) = doc.get_str("sql").ok() else {
            return Err("runCommand requires sql".into());
        };
        if doc.len() != 1 {
            return Err("runCommand accepts sql".into());
        }
        return Ok(Command::Sql {
            statement: statement.to_string(),
        });
    }
    let Some((collection, after)) = rest.split_once('.') else {
        return Err(unknown());
    };
    if !is_name(collection) {
        return Err(unknown());
    }
    let Some((method, args_src)) = after.split_once('(') else {
        return Err(unknown());
    };
    let Some(inner) = args_src.strip_suffix(')') else {
        return Err(unknown());
    };
    if !balanced(&format!("({inner})")) {
        return Err(unknown());
    }
    let args = split_args(inner)?;
    match method {
        "find" => match args.as_slice() {
            [] => Ok(Command::Find {
                collection: collection.to_string(),
                filter: Document::new(),
            }),
            [filter] => Ok(Command::Find {
                collection: collection.to_string(),
                filter: parse_document(filter)?,
            }),
            _ => Err(unknown()),
        },
        "insertOne" => match args.as_slice() {
            [document] => Ok(Command::InsertOne {
                collection: collection.to_string(),
                document: parse_document(document)?,
            }),
            _ => Err(unknown()),
        },
        "updateOne" => match args.as_slice() {
            [filter, update] => Ok(Command::UpdateOne {
                collection: collection.to_string(),
                filter: parse_document(filter)?,
                update: parse_document(update)?,
            }),
            _ => Err(unknown()),
        },
        "deleteOne" => match args.as_slice() {
            [filter] => Ok(Command::DeleteOne {
                collection: collection.to_string(),
                filter: parse_document(filter)?,
            }),
            _ => Err(unknown()),
        },
        _ => Err(unknown()),
    }
}

fn call_args(after_name: &str) -> Option<Vec<String>> {
    let after_name = after_name.trim_start();
    let inner = after_name.strip_prefix('(')?.strip_suffix(')')?;
    if !balanced(&format!("({inner})")) {
        return None;
    }
    split_args(inner).ok()
}

fn balanced(text: &str) -> bool {
    let mut depth = 0i32;
    let mut quote = false;
    let mut escape = false;
    for ch in text.chars() {
        if quote {
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                quote = false;
            }
            continue;
        }
        match ch {
            '"' => quote = true,
            '{' | '[' | '(' => depth += 1,
            '}' | ']' | ')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && !quote
}

fn split_args(inner: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;
    let mut quote = false;
    let mut escape = false;
    let bytes = inner.as_bytes();
    for (index, &byte) in bytes.iter().enumerate() {
        if quote {
            if escape {
                escape = false;
                continue;
            }
            if byte == b'\\' {
                escape = true;
                continue;
            }
            if byte == b'"' {
                quote = false;
            }
            continue;
        }
        match byte {
            b'"' => quote = true,
            b'{' | b'[' | b'(' => depth += 1,
            b'}' | b']' | b')' => depth -= 1,
            b',' if depth == 0 => {
                args.push(inner[start..index].trim().to_string());
                start = index + 1;
            }
            _ => {}
        }
    }
    if quote || depth != 0 {
        return Err(format!("unknown command: {inner}"));
    }
    let tail = inner[start..].trim();
    if !tail.is_empty() {
        args.push(tail.to_string());
    }
    Ok(args)
}

pub fn is_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub fn parse_document(text: &str) -> Result<Document, String> {
    match parse_bson(text)? {
        Bson::Document(document) => Ok(document),
        _ => Err("expected a document".into()),
    }
}

pub fn parse_bson(text: &str) -> Result<Bson, String> {
    let mut parser = Parser::new(text);
    let value = parser.parse_value()?;
    parser.skip();
    if parser.rest().is_empty() {
        Ok(value)
    } else {
        Err("trailing characters".into())
    }
}

struct Parser<'a> {
    src: &'a str,
    index: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self { src, index: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.src[self.index..]
    }

    fn skip(&mut self) {
        while self
            .rest()
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            self.index += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.index += ch.len_utf8();
        Some(ch)
    }

    fn parse_value(&mut self) -> Result<Bson, String> {
        self.skip();
        match self.peek() {
            Some('{') => Ok(extend(self.parse_object()?)),
            Some('[') => self.parse_array(),
            Some('"') => Ok(Bson::String(self.parse_string()?)),
            Some('-') => self.parse_number(),
            Some(ch) if ch.is_ascii_digit() => self.parse_number(),
            Some('t') if self.rest().starts_with("true") => {
                self.index += 4;
                Ok(Bson::Boolean(true))
            }
            Some('f') if self.rest().starts_with("false") => {
                self.index += 5;
                Ok(Bson::Boolean(false))
            }
            Some('n') if self.rest().starts_with("null") => {
                self.index += 4;
                Ok(Bson::Null)
            }
            _ => Err("expected a value".into()),
        }
    }

    fn parse_object(&mut self) -> Result<Document, String> {
        self.bump();
        let mut document = Document::new();
        loop {
            self.skip();
            if self.peek() == Some('}') {
                self.bump();
                return Ok(document);
            }
            let key = self.parse_key()?;
            self.skip();
            if self.bump() != Some(':') {
                return Err(format!("missing ':' after {key}"));
            }
            let value = self.parse_value()?;
            document.insert(key, value);
            self.skip();
            match self.peek() {
                Some(',') => {
                    self.bump();
                }
                Some('}') => {}
                _ => return Err("expected ',' or '}'".into()),
            }
        }
    }

    fn parse_array(&mut self) -> Result<Bson, String> {
        self.bump();
        let mut values = Vec::new();
        loop {
            self.skip();
            if self.peek() == Some(']') {
                self.bump();
                return Ok(Bson::Array(values));
            }
            values.push(self.parse_value()?);
            self.skip();
            match self.peek() {
                Some(',') => {
                    self.bump();
                }
                Some(']') => {}
                _ => return Err("expected ',' or ']'".into()),
            }
        }
    }

    fn parse_key(&mut self) -> Result<String, String> {
        self.skip();
        if self.peek() == Some('"') {
            return self.parse_string();
        }
        let start = self.index;
        let bytes = self.src.as_bytes();
        if start >= bytes.len() {
            return Err("missing key".into());
        }
        let first = bytes[start];
        if !(first.is_ascii_alphabetic() || first == b'_' || first == b'$') {
            return Err("invalid key".into());
        }
        self.index += 1;
        while self.index < bytes.len() {
            let byte = bytes[self.index];
            if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' {
                self.index += 1;
            } else {
                break;
            }
        }
        Ok(self.src[start..self.index].to_string())
    }

    fn parse_string(&mut self) -> Result<String, String> {
        if self.bump() != Some('"') {
            return Err("expected a string".into());
        }
        let mut out = String::new();
        loop {
            match self.bump() {
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('/') => out.push('/'),
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some(_) => return Err("bad string escape".into()),
                    None => return Err("unterminated string".into()),
                },
                Some(ch) => out.push(ch),
                None => return Err("unterminated string".into()),
            }
        }
    }

    fn parse_number(&mut self) -> Result<Bson, String> {
        let start = self.index;
        if self.peek() == Some('-') {
            self.bump();
        }
        if !self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            return Err("invalid number".into());
        }
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.bump();
        }
        let mut float = false;
        if self.peek() == Some('.') {
            float = true;
            self.bump();
            if !self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                return Err("invalid number".into());
            }
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.bump();
            }
        }
        let text = &self.src[start..self.index];
        if float {
            let number: f64 = text.parse().map_err(|_| "invalid number".to_string())?;
            return Ok(Bson::Double(number));
        }
        if let Ok(number) = text.parse::<i32>() {
            return Ok(Bson::Int32(number));
        }
        if let Ok(number) = text.parse::<i64>() {
            return Ok(Bson::Int64(number));
        }
        Err("invalid number".into())
    }
}

/// Turns a one-key extended JSON object into the BSON it names.
fn extend(document: Document) -> Bson {
    if document.len() == 1 {
        if let Ok(hex) = document.get_str("$oid") {
            if let Ok(oid) = ObjectId::parse_str(hex) {
                return Bson::ObjectId(oid);
            }
        }
    }
    Bson::Document(document)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_shell_commands() {
        assert_eq!(parse_command("?").unwrap(), Command::Help);
        assert_eq!(parse_command("help").unwrap(), Command::Help);
        assert_eq!(parse_command("show dbs;").unwrap(), Command::ShowDbs);
        assert_eq!(parse_command("show dbs").unwrap(), Command::ShowDbs);
        assert_eq!(
            parse_command("use shop").unwrap(),
            Command::Use {
                database: "shop".into()
            }
        );
        assert_eq!(
            parse_command("show collections").unwrap(),
            Command::ShowCollections
        );
        match parse_command("db.orders.find()").unwrap() {
            Command::Find { collection, filter } => {
                assert_eq!(collection, "orders");
                assert!(filter.is_empty());
            }
            other => panic!("{other:?}"),
        }
        match parse_command("db.orders.find({ sku: 1 })").unwrap() {
            Command::Find { filter, .. } => assert_eq!(filter.get_i32("sku").unwrap(), 1),
            other => panic!("{other:?}"),
        }
        match parse_command("db.orders.insertOne({ sku: 9, qty: 1 })").unwrap() {
            Command::InsertOne { document, .. } => {
                assert_eq!(document.get_i32("sku").unwrap(), 9);
                assert_eq!(document.get_i32("qty").unwrap(), 1);
            }
            other => panic!("{other:?}"),
        }
        match parse_command("db.orders.updateOne({ sku: 9 }, { $set: { qty: 2 } })").unwrap() {
            Command::UpdateOne { update, .. } => {
                assert_eq!(
                    update.get_document("$set").unwrap().get_i32("qty").unwrap(),
                    2
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse_command("db.orders.deleteOne({ sku: 9 })").unwrap(),
            Command::DeleteOne { .. }
        ));
        match parse_command(r#"db.runCommand({ sql: "select sku from orders" })"#).unwrap() {
            Command::Sql { statement } => assert_eq!(statement, "select sku from orders"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_unknown_line_names_itself() {
        let err = parse_command("please delete everything").unwrap_err();
        assert_eq!(err, "unknown command: please delete everything");
    }

    #[test]
    fn quoted_json_round_trips_an_object_id() {
        let oid = ObjectId::parse_str("674a1c0e0e0e0e0e0e0e0e0e").unwrap();
        let text = format!(r#"{{ "_id": {{ "$oid": "{oid}" }}, "qty": 2 }}"#);
        let document = parse_document(&text).unwrap();
        assert_eq!(document.get_object_id("_id").unwrap(), oid);
        assert_eq!(document.get_i32("qty").unwrap(), 2);
    }
}
