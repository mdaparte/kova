//! Plain SQL dumps.
//!
//! PostgreSQL is the text written by `pg_dump` (not the custom archive):
//! `CREATE TABLE`, `COPY ... FROM stdin` in text format ended by `\.`,
//! and `INSERT INTO ... VALUES`. The schema name is dropped. INSERT and
//! COPY use the column type from `CREATE TABLE`: `json` and `jsonb`
//! become documents. A value is also read from its shape: a date, a
//! number, `true`/`false`, or a JSON object or array, including inside
//! other JSON. `integer[]` or `text[]` become arrays. `SET`,
//! `ALTER`, `CREATE INDEX`,
//! dollar-quoted function bodies, and psql commands such as `\restrict`
//! are skipped.
//!
//! MySQL is the text written by `mysqldump`: backtick identifiers,
//! `CREATE TABLE`, and extended `INSERT` statements. String literals use
//! backslash escapes. `LOCK TABLES` is skipped.

use std::collections::BTreeMap;

use bson::{Bson, Document};

use super::{check_name, CollectionDump, Dump};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Dialect {
    Postgres,
    Mysql,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeKind {
    Int,
    Float,
    Bool,
    Json,
    Date,
    Text,
}

struct Column {
    name: String,
    kind: TypeKind,
    array: bool,
}

pub(super) fn detect(text: &str) -> Result<Dialect, String> {
    let head_end = floor_char(text, text.len().min(8192));
    let head = &text[..head_end];
    if head.contains("PostgreSQL database dump") {
        return Ok(Dialect::Postgres);
    }
    if head.contains("MySQL dump") || head.contains("MariaDB dump") {
        return Ok(Dialect::Mysql);
    }
    if text.contains("FROM stdin") {
        return Ok(Dialect::Postgres);
    }
    if text.contains("INSERT INTO `") || text.contains("CREATE TABLE `") {
        return Ok(Dialect::Mysql);
    }
    if text.contains("INSERT INTO") || text.contains("CREATE TABLE") {
        return Ok(Dialect::Postgres);
    }
    Err("unrecognized dump. Expected a Kova dump, a mongodump directory or archive, pg_dump plain SQL, or mysqldump SQL".into())
}

pub(super) fn parse(text: &str, dialect: Dialect) -> Result<Dump, String> {
    let mut sql = Sql { text, index: 0 };
    let mut columns: BTreeMap<String, Vec<Column>> = BTreeMap::new();
    let mut dump = Dump::default();
    while !sql.eof() {
        sql.skip()?;
        if sql.eof() {
            break;
        }
        if sql.peek() == ';' {
            sql.bump();
            continue;
        }
        if sql.peek() == '\\' {
            sql.skip_psql_command();
            continue;
        }
        let word = sql.read_word()?;
        if word.eq_ignore_ascii_case("create") {
            sql.parse_create(&mut columns, &mut dump)?;
        } else if word.eq_ignore_ascii_case("insert") {
            sql.parse_insert(dialect, &columns, &mut dump)?;
        } else if word.eq_ignore_ascii_case("copy") && dialect == Dialect::Postgres {
            sql.parse_copy(&columns, &mut dump)?;
        } else {
            sql.skip_statement()?;
        }
    }
    Ok(dump)
}

struct Sql<'a> {
    text: &'a str,
    index: usize,
}

impl Sql<'_> {
    fn parse_create(
        &mut self,
        columns: &mut BTreeMap<String, Vec<Column>>,
        dump: &mut Dump,
    ) -> Result<(), String> {
        let mut word = self.read_word()?;
        while matches_modifier(&word) {
            word = self.read_word()?;
        }
        if !word.eq_ignore_ascii_case("table") {
            self.skip_statement()?;
            return Ok(());
        }
        if self.peek_word("if") {
            self.read_word()?;
            let not = self.read_word()?;
            let exists = self.read_word()?;
            if !not.eq_ignore_ascii_case("not") || !exists.eq_ignore_ascii_case("exists") {
                return Err(format!("expected IF NOT EXISTS {}", self.here()));
            }
        }
        let name = self.read_qualified()?;
        check_name(&name)?;
        self.skip()?;
        if !self.eat('(') {
            return Err(format!(
                "CREATE TABLE {name} is missing its column list {}",
                self.here()
            ));
        }
        let declared = self.read_columns()?;
        self.skip_statement()?;
        columns.insert(name.clone(), declared);
        if !dump
            .collections
            .iter()
            .any(|collection| collection.name == name)
        {
            dump.collections.push(CollectionDump {
                name,
                documents: Vec::new(),
                indexes: Vec::new(),
            });
        }
        Ok(())
    }

    fn parse_insert(
        &mut self,
        dialect: Dialect,
        columns: &BTreeMap<String, Vec<Column>>,
        dump: &mut Dump,
    ) -> Result<(), String> {
        let mut word = self.read_word()?;
        while matches!(
            word.to_ascii_lowercase().as_str(),
            "ignore" | "low_priority" | "delayed" | "high_priority"
        ) {
            word = self.read_word()?;
        }
        if word.eq_ignore_ascii_case("into") {
            word = String::new();
        }
        let name = if word.is_empty() {
            self.read_qualified()?
        } else {
            // `INSERT table` is not a dump we accept. The word was the table
            // only when it was not a modifier and not INTO; read_qualified
            // needs to see the identifier, so this path is the identifier
            // already consumed. Re-parse is unnecessary: `word` is the name
            // when the statement omitted INTO, which mysqldump does not do.
            word
        };
        check_name(&name)?;
        self.skip()?;
        let explicit = if self.eat('(') {
            Some(self.read_name_list()?)
        } else {
            None
        };
        let values = self.read_word()?;
        if !values.eq_ignore_ascii_case("values") && !values.eq_ignore_ascii_case("value") {
            return Err(format!("INSERT INTO {name} has no VALUES {}", self.here()));
        }
        loop {
            self.skip()?;
            if !self.eat('(') {
                return Err(format!(
                    "INSERT INTO {name} is missing a row {}",
                    self.here()
                ));
            }
            let row = self.read_value_list(dialect)?;
            self.push_row(&name, explicit.as_deref(), &row, columns, dump)?;
            self.skip()?;
            if self.eat(',') {
                continue;
            }
            if self.eat(';') {
                break;
            }
            return Err(format!(
                "INSERT INTO {name} row is not followed by ',' or ';' {}",
                self.here()
            ));
        }
        Ok(())
    }

    fn parse_copy(
        &mut self,
        columns: &BTreeMap<String, Vec<Column>>,
        dump: &mut Dump,
    ) -> Result<(), String> {
        let name = self.read_qualified()?;
        check_name(&name)?;
        self.skip()?;
        let explicit = if self.eat('(') {
            Some(self.read_name_list()?)
        } else {
            None
        };
        let from = self.read_word()?;
        let stdin = self.read_word()?;
        if !from.eq_ignore_ascii_case("from") || !stdin.eq_ignore_ascii_case("stdin") {
            return Err(format!("COPY {name} is not FROM stdin {}", self.here()));
        }
        self.skip()?;
        if self.peek_word("with") || self.peek_word("binary") {
            return Err(format!(
                "COPY {name} binary format is not restored {}",
                self.here()
            ));
        }
        if !self.eat(';') {
            return Err(format!("COPY {name} is missing ';' {}", self.here()));
        }
        self.finish_line();
        loop {
            if self.eof() {
                return Err(format!("COPY {name} ended without \\."));
            }
            let line = self.read_line();
            if line == "\\." {
                break;
            }
            let fields: Vec<String> = line.split('\t').map(str::to_string).collect();
            let values = fields
                .iter()
                .map(|field| coerce_copy(field))
                .collect::<Result<Vec<_>, _>>()?;
            self.push_row(&name, explicit.as_deref(), &values, columns, dump)?;
        }
        Ok(())
    }

    fn push_row(
        &mut self,
        name: &str,
        explicit: Option<&[String]>,
        values: &[Bson],
        columns: &BTreeMap<String, Vec<Column>>,
        dump: &mut Dump,
    ) -> Result<(), String> {
        let declared = columns.get(name);
        let names: Vec<(String, TypeKind, bool)> = if let Some(explicit) = explicit {
            explicit
                .iter()
                .map(|column| {
                    let found = declared.and_then(|columns| {
                        columns.iter().find(|declared| declared.name == *column)
                    });
                    (
                        column.clone(),
                        found.map(|column| column.kind).unwrap_or(TypeKind::Text),
                        found.is_some_and(|column| column.array),
                    )
                })
                .collect()
        } else {
            declared
                .ok_or_else(|| {
                    format!(
                        "{name} has no column names; the dump needs CREATE TABLE or a column list"
                    )
                })?
                .iter()
                .map(|column| (column.name.clone(), column.kind, column.array))
                .collect()
        };
        if names.len() != values.len() {
            return Err(format!(
                "{name} has {} columns and a row of {} values",
                names.len(),
                values.len()
            ));
        }
        let mut document = Document::new();
        for ((column, kind, array), value) in names.into_iter().zip(values.iter()) {
            let value = match value {
                Bson::String(text) => coerce_value(text, kind, array)
                    .map_err(|err| format!("{column}: {err}"))?,
                other => other.clone(),
            };
            document.insert(column, value);
        }
        let collection = if let Some(collection) = dump
            .collections
            .iter_mut()
            .find(|collection| collection.name == name)
        {
            collection
        } else {
            dump.collections.push(CollectionDump {
                name: name.to_string(),
                documents: Vec::new(),
                indexes: Vec::new(),
            });
            dump.collections.last_mut().expect("just pushed")
        };
        collection.documents.push(document);
        Ok(())
    }

    fn read_columns(&mut self) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();
        loop {
            self.skip()?;
            if self.eat(')') {
                break;
            }
            if self.eat(',') {
                continue;
            }
            let name = self.read_ident()?;
            if is_table_constraint(&name) {
                match self.skip_until_comma_or_close()? {
                    ',' => {
                        self.bump();
                        continue;
                    }
                    ')' => {
                        self.bump();
                        break;
                    }
                    _ => {}
                }
                continue;
            }
            let type_text = self.read_type_text()?;
            let array = self.take_array_markers()?;
            columns.push(Column {
                name,
                kind: type_kind(&type_text),
                array,
            });
            self.skip()?;
            if self.peek() == ',' || self.peek() == ')' {
                continue;
            }
            match self.skip_until_comma_or_close()? {
                ',' => {
                    self.bump();
                }
                ')' => {
                    self.bump();
                    break;
                }
                _ => {}
            }
        }
        if columns.is_empty() {
            return Err(format!("CREATE TABLE has no columns {}", self.here()));
        }
        Ok(columns)
    }

    fn read_type_text(&mut self) -> Result<String, String> {
        let mut parts = Vec::new();
        loop {
            self.skip()?;
            if self.eof() {
                break;
            }
            let ch = self.peek();
            if ch == '(' {
                parts.push(self.read_balanced()?);
                continue;
            }
            if !is_word_start(ch) {
                break;
            }
            if is_column_constraint(&self.peek_word_text()) {
                break;
            }
            parts.push(self.read_word()?);
        }
        if parts.is_empty() {
            return Err(format!("column is missing a type {}", self.here()));
        }
        Ok(parts.join(" "))
    }

    /// `integer[]` and `text ARRAY` are arrays of the element type.
    fn take_array_markers(&mut self) -> Result<bool, String> {
        let mut array = false;
        loop {
            self.skip()?;
            if self.eof() {
                break;
            }
            if self.peek() == '[' {
                array = true;
                self.bump();
                while !self.eof() && self.peek() != ']' {
                    self.bump();
                }
                if !self.eat(']') {
                    return Err(format!("unclosed array type {}", self.here()));
                }
                continue;
            }
            if self.peek_word("array") {
                array = true;
                let _ = self.read_word()?;
                continue;
            }
            break;
        }
        Ok(array)
    }

    fn read_balanced(&mut self) -> Result<String, String> {
        let start = self.index;
        if !self.eat('(') {
            return Err(format!("expected '(' {}", self.here()));
        }
        let mut depth = 1i32;
        while depth > 0 {
            if self.eof() {
                return Err(format!("unclosed '(' {}", self.here()));
            }
            let ch = self.peek();
            self.bump();
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                '\'' => {
                    self.index -= ch.len_utf8();
                    self.read_quoted_string(false)?;
                }
                _ => {}
            }
        }
        Ok(self.text[start..self.index].to_string())
    }

    fn read_name_list(&mut self) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        loop {
            self.skip()?;
            if self.eat(')') {
                break;
            }
            if self.eat(',') {
                continue;
            }
            names.push(self.read_ident()?);
        }
        if names.is_empty() {
            return Err(format!("empty column list {}", self.here()));
        }
        Ok(names)
    }

    fn read_value_list(&mut self, dialect: Dialect) -> Result<Vec<Bson>, String> {
        let mut values = Vec::new();
        loop {
            self.skip()?;
            if self.eat(')') {
                break;
            }
            if self.eat(',') {
                continue;
            }
            values.push(self.read_value(dialect)?);
            self.skip()?;
            if self.starts_with("::") {
                self.index += 2;
                let _ = self.read_type_text()?;
            }
        }
        Ok(values)
    }

    fn read_value(&mut self, dialect: Dialect) -> Result<Bson, String> {
        self.skip()?;
        if self.eof() {
            return Err(format!("expected a value {}", self.here()));
        }
        let ch = self.peek();
        if ch == '\'' {
            return Ok(Bson::String(
                self.read_quoted_string(dialect == Dialect::Mysql)?,
            ));
        }
        if ch == '-' || ch == '+' || ch.is_ascii_digit() {
            return self.read_number();
        }
        if (ch == 'E' || ch == 'e') && self.second_is('\'') {
            self.bump();
            return Ok(Bson::String(self.read_quoted_string(true)?));
        }
        if !is_word_start(ch) {
            return Err(format!("unsupported value {}", self.here()));
        }
        let word = self.read_word()?;
        if word.eq_ignore_ascii_case("null") || word.eq_ignore_ascii_case("default") {
            return Ok(Bson::Null);
        }
        if word.eq_ignore_ascii_case("true") {
            return Ok(Bson::Boolean(true));
        }
        if word.eq_ignore_ascii_case("false") {
            return Ok(Bson::Boolean(false));
        }
        Err(format!("unsupported value {word} {}", self.here()))
    }

    fn read_number(&mut self) -> Result<Bson, String> {
        let start = self.index;
        if self.peek() == '+' || self.peek() == '-' {
            self.bump();
        }
        let mut saw_fraction = false;
        let mut digits = false;
        while !self.eof() && self.peek().is_ascii_digit() {
            digits = true;
            self.bump();
        }
        if !self.eof() && self.peek() == '.' {
            saw_fraction = true;
            self.bump();
            while !self.eof() && self.peek().is_ascii_digit() {
                digits = true;
                self.bump();
            }
        }
        if !self.eof() && (self.peek() == 'e' || self.peek() == 'E') {
            saw_fraction = true;
            self.bump();
            if !self.eof() && (self.peek() == '+' || self.peek() == '-') {
                self.bump();
            }
            while !self.eof() && self.peek().is_ascii_digit() {
                digits = true;
                self.bump();
            }
        }
        if !digits {
            return Err(format!("bad number {}", self.here()));
        }
        let token = &self.text[start..self.index];
        if saw_fraction {
            let number: f64 = token.parse().map_err(|_| format!("bad number {token}"))?;
            Ok(Bson::Double(number))
        } else {
            parse_int(token)
        }
    }

    fn read_qualified(&mut self) -> Result<String, String> {
        let mut name = self.read_ident()?;
        loop {
            self.skip()?;
            if !self.eat('.') {
                break;
            }
            name = self.read_ident()?;
        }
        if name.is_empty() {
            return Err(format!("empty table name {}", self.here()));
        }
        Ok(name)
    }

    fn read_ident(&mut self) -> Result<String, String> {
        self.skip()?;
        if self.eof() {
            return Err(format!("expected an identifier {}", self.here()));
        }
        let ch = self.peek();
        if ch == '"' || ch == '`' {
            return self.read_quoted_ident(ch);
        }
        if !is_word_start(ch) {
            return Err(format!("expected an identifier {}", self.here()));
        }
        self.read_word()
    }

    fn read_quoted_ident(&mut self, quote: char) -> Result<String, String> {
        self.bump();
        let mut out = String::new();
        loop {
            if self.eof() {
                return Err(format!("unclosed identifier {}", self.here()));
            }
            let ch = self.peek();
            self.bump();
            if ch == quote {
                if !self.eof() && self.peek() == quote {
                    self.bump();
                    out.push(quote);
                    continue;
                }
                return Ok(out);
            }
            out.push(ch);
        }
    }

    fn read_quoted_string(&mut self, escapes: bool) -> Result<String, String> {
        if !self.eat('\'') {
            return Err(format!("expected a string {}", self.here()));
        }
        let mut out = String::new();
        loop {
            if self.eof() {
                return Err(format!("unclosed string {}", self.here()));
            }
            let ch = self.peek();
            self.bump();
            if ch == '\'' {
                if !self.eof() && self.peek() == '\'' {
                    self.bump();
                    out.push('\'');
                    continue;
                }
                return Ok(out);
            }
            if escapes && ch == '\\' {
                if self.eof() {
                    return Err("trailing backslash in a string".into());
                }
                let next = self.peek();
                self.bump();
                out.push(match next {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'Z' => '\u{1a}',
                    other => other,
                });
                continue;
            }
            out.push(ch);
        }
    }

    fn skip_until_comma_or_close(&mut self) -> Result<char, String> {
        let mut depth = 0i32;
        loop {
            if self.eof() {
                return Err(format!("unclosed column {}", self.here()));
            }
            let ch = self.peek();
            if ch == '\'' {
                self.read_quoted_string(false)?;
                continue;
            }
            if ch == '"' || ch == '`' {
                self.read_quoted_ident(ch)?;
                continue;
            }
            if ch == '(' {
                depth += 1;
                self.bump();
                continue;
            }
            if ch == ')' {
                if depth == 0 {
                    return Ok(')');
                }
                depth -= 1;
                self.bump();
                continue;
            }
            if ch == ',' && depth == 0 {
                return Ok(',');
            }
            self.bump();
        }
    }

    fn skip_statement(&mut self) -> Result<(), String> {
        while !self.eof() {
            self.skip()?;
            if self.eof() {
                break;
            }
            let ch = self.peek();
            if ch == ';' {
                self.bump();
                return Ok(());
            }
            if ch == '\'' {
                self.read_quoted_string(false)?;
                continue;
            }
            if ch == '"' || ch == '`' {
                self.read_quoted_ident(ch)?;
                continue;
            }
            if ch == '$' && self.skip_dollar_quote()? {
                continue;
            }
            self.bump();
        }
        Err(format!("statement ended without ';' {}", self.here()))
    }

    fn skip_dollar_quote(&mut self) -> Result<bool, String> {
        let rest = &self.text[self.index + 1..];
        let mut tag_len = 0usize;
        for ch in rest.chars() {
            if ch == '$' {
                break;
            }
            if ch.is_ascii_alphanumeric() || ch == '_' {
                tag_len += ch.len_utf8();
            } else {
                return Ok(false);
            }
        }
        if tag_len == rest.len() || !rest[tag_len..].starts_with('$') {
            return Ok(false);
        }
        let closer = &self.text[self.index..self.index + 1 + tag_len + 1];
        let body_at = self.index + closer.len();
        let Some(found) = self.text[body_at..].find(closer) else {
            return Err(format!("unclosed dollar quote {}", self.here()));
        };
        self.index = body_at + found + closer.len();
        Ok(true)
    }

    /// A psql meta-command (`\restrict`, `\unrestrict`) occupies one line.
    fn skip_psql_command(&mut self) {
        while !self.eof() && self.peek() != '\n' {
            self.bump();
        }
    }

    fn skip(&mut self) -> Result<(), String> {
        loop {
            while !self.eof() && self.peek().is_whitespace() {
                self.bump();
            }
            if self.starts_with("--") || (!self.eof() && self.peek() == '#') {
                while !self.eof() && self.peek() != '\n' {
                    self.bump();
                }
                continue;
            }
            if self.starts_with("/*") {
                self.index += 2;
                let Some(end) = self.text[self.index..].find("*/") else {
                    return Err("unclosed block comment".into());
                };
                self.index += end + 2;
                continue;
            }
            return Ok(());
        }
    }

    fn read_word(&mut self) -> Result<String, String> {
        self.skip()?;
        if self.eof() || !is_word_start(self.peek()) {
            return Err(format!("expected a word {}", self.here()));
        }
        let start = self.index;
        self.bump();
        while !self.eof() && is_word_continue(self.peek()) {
            self.bump();
        }
        Ok(self.text[start..self.index].to_string())
    }

    fn peek_word(&mut self, word: &str) -> bool {
        let saved = self.index;
        let matched = self
            .read_word()
            .ok()
            .is_some_and(|found| found.eq_ignore_ascii_case(word));
        self.index = saved;
        matched
    }

    fn peek_word_text(&self) -> String {
        let mut index = self.index;
        let mut out = String::new();
        for ch in self.text[index..].chars() {
            if out.is_empty() && !is_word_start(ch) {
                break;
            }
            if !out.is_empty() && !is_word_continue(ch) {
                break;
            }
            out.push(ch);
            index += ch.len_utf8();
        }
        let _ = index;
        out
    }

    fn read_line(&mut self) -> String {
        let start = self.index;
        while !self.eof() && self.peek() != '\n' {
            self.bump();
        }
        let mut line = &self.text[start..self.index];
        if !self.eof() && self.peek() == '\n' {
            self.bump();
        }
        if let Some(stripped) = line.strip_suffix('\r') {
            line = stripped;
        }
        line.to_string()
    }

    fn finish_line(&mut self) {
        if !self.eof() && self.peek() == '\r' {
            self.bump();
        }
        if !self.eof() && self.peek() == '\n' {
            self.bump();
        }
    }

    fn peek(&self) -> char {
        self.text[self.index..].chars().next().unwrap_or('\0')
    }

    fn second_is(&self, ch: char) -> bool {
        self.text[self.index..].chars().nth(1) == Some(ch)
    }

    fn eat(&mut self, ch: char) -> bool {
        if !self.eof() && self.peek() == ch {
            self.bump();
            true
        } else {
            false
        }
    }

    fn bump(&mut self) {
        if let Some(ch) = self.text[self.index..].chars().next() {
            self.index += ch.len_utf8();
        }
    }

    fn starts_with(&self, prefix: &str) -> bool {
        self.text[self.index..].starts_with(prefix)
    }

    fn eof(&self) -> bool {
        self.index >= self.text.len()
    }

    fn here(&self) -> String {
        let start = floor_char(self.text, self.index.saturating_sub(24));
        let end = floor_char(self.text, (self.index + 24).min(self.text.len()));
        format!("near byte {}: {:?}", self.index, &self.text[start..end])
    }
}

fn matches_modifier(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "temp" | "temporary" | "unlogged" | "global" | "local"
    )
}

fn is_table_constraint(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "primary"
            | "unique"
            | "key"
            | "constraint"
            | "foreign"
            | "check"
            | "index"
            | "fulltext"
            | "spatial"
    )
}

fn is_column_constraint(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "not"
            | "null"
            | "default"
            | "primary"
            | "unique"
            | "key"
            | "check"
            | "references"
            | "constraint"
            | "collate"
            | "comment"
            | "auto_increment"
            | "generated"
            | "stored"
            | "virtual"
            | "invisible"
    )
}

fn type_kind(sql_type: &str) -> TypeKind {
    let word = sql_type
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .find(|word| !word.is_empty())
        .unwrap_or("")
        .to_ascii_lowercase();
    match word.as_str() {
        "int" | "integer" | "bigint" | "smallint" | "tinyint" | "mediumint" | "serial"
        | "bigserial" => TypeKind::Int,
        "bool" | "boolean" => TypeKind::Bool,
        "float" | "double" | "real" | "numeric" | "decimal" => TypeKind::Float,
        "json" | "jsonb" => TypeKind::Json,
        "date" | "timestamp" | "timestamptz" | "datetime" => TypeKind::Date,
        _ => TypeKind::Text,
    }
}

fn coerce_copy(field: &str) -> Result<Bson, String> {
    if field == "\\N" {
        return Ok(Bson::Null);
    }
    Ok(Bson::String(unescape_copy(field)?))
}

fn coerce_value(text: &str, kind: TypeKind, array: bool) -> Result<Bson, String> {
    if array {
        return parse_pg_array(text, kind);
    }
    coerce_text(text, kind)
}

fn coerce_text(text: &str, kind: TypeKind) -> Result<Bson, String> {
    match kind {
        TypeKind::Text => Ok(infer_text(text)),
        TypeKind::Int => {
            if text.contains(['.', 'e', 'E']) {
                parse_float(text)
            } else {
                parse_int(text)
            }
        }
        TypeKind::Float => parse_float(text),
        TypeKind::Bool => parse_bool(text),
        TypeKind::Json => parse_json_text(text),
        TypeKind::Date => parse_sql_datetime(text),
    }
}

fn parse_pg_array(text: &str, kind: TypeKind) -> Result<Bson, String> {
    let text = text.trim();
    if text.len() < 2 || !text.starts_with('{') || !text.ends_with('}') {
        return Err(format!("bad array {text}"));
    }
    let inner = &text[1..text.len() - 1];
    if inner.is_empty() {
        return Ok(Bson::Array(Vec::new()));
    }
    let mut items = Vec::new();
    for element in split_pg_array(inner)? {
        items.push(parse_pg_element(element.trim(), kind)?);
    }
    Ok(Bson::Array(items))
}

fn parse_pg_element(element: &str, kind: TypeKind) -> Result<Bson, String> {
    if element == "NULL" {
        return Ok(Bson::Null);
    }
    if element.starts_with('{') {
        return parse_pg_array(element, kind);
    }
    let text = if element.starts_with('"') {
        unescape_pg_quoted(element)?
    } else {
        element.to_string()
    };
    coerce_text(&text, kind)
}

fn split_pg_array(inner: &str) -> Result<Vec<String>, String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut quoted = false;
    let mut escape = false;
    for ch in inner.chars() {
        if escape {
            current.push(ch);
            escape = false;
            continue;
        }
        if quoted {
            current.push(ch);
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                quoted = false;
            }
            continue;
        }
        match ch {
            '"' => {
                quoted = true;
                current.push(ch);
            }
            '{' => {
                depth += 1;
                current.push(ch);
            }
            '}' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!("bad array {{{inner}}}"));
                }
                current.push(ch);
            }
            ',' if depth == 0 => items.push(std::mem::take(&mut current)),
            _ => current.push(ch),
        }
    }
    if quoted || depth != 0 || escape {
        return Err(format!("bad array {{{inner}}}"));
    }
    items.push(current);
    Ok(items)
}

fn unescape_pg_quoted(element: &str) -> Result<String, String> {
    let body = element
        .strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
        .ok_or_else(|| format!("bad array element {element}"))?;
    let mut out = String::new();
    let mut chars = body.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            out.push(chars.next().ok_or("trailing backslash in an array element")?);
            continue;
        }
        out.push(ch);
    }
    Ok(out)
}

fn unescape_copy(field: &str) -> Result<String, String> {
    if !field.contains('\\') {
        return Ok(field.to_string());
    }
    let mut out = String::new();
    let mut chars = field.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some(other) => out.push(other),
            None => return Err("trailing backslash in a COPY field".into()),
        }
    }
    Ok(out)
}

fn parse_int(token: &str) -> Result<Bson, String> {
    let number: i64 = token.parse().map_err(|_| format!("bad integer {token}"))?;
    if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&number) {
        Ok(Bson::Int32(number as i32))
    } else {
        Ok(Bson::Int64(number))
    }
}

fn parse_float(token: &str) -> Result<Bson, String> {
    let number: f64 = token.parse().map_err(|_| format!("bad number {token}"))?;
    Ok(Bson::Double(number))
}

fn parse_json_text(text: &str) -> Result<Bson, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|err| format!("bad json: {err}"))?;
    let mut parsed = bson::to_bson(&value).map_err(|err| format!("bad json: {err}"))?;
    refine(&mut parsed);
    Ok(parsed)
}

/// Reads a value from its shape. A date, a number, `true`/`false`, or a
/// JSON object or array becomes that type. A number that keeps a leading
/// zero, such as `00004345951`, stays text. The same reading applies
/// inside every nested document and array.
fn infer_text(text: &str) -> Bson {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Bson::String(text.to_string());
    }
    if let Ok(date) = parse_sql_datetime(trimmed) {
        return date;
    }
    if let Some(number) = infer_number(trimmed) {
        return number;
    }
    if let Some(flag) = infer_bool(trimmed) {
        return Bson::Boolean(flag);
    }
    if looks_like_json(trimmed) {
        if let Ok(parsed) = parse_json_text(trimmed) {
            return parsed;
        }
    }
    Bson::String(text.to_string())
}

fn infer_number(text: &str) -> Option<Bson> {
    let unsigned = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    if whole.is_empty() || !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if let Some(fraction) = fraction {
        if fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    if whole.len() > 1 && whole.starts_with('0') {
        return None;
    }
    if fraction.is_some() {
        parse_float(text).ok()
    } else {
        parse_int(text).ok()
    }
}

fn infer_bool(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn looks_like_json(text: &str) -> bool {
    let bytes = text.as_bytes();
    matches!(
        (bytes.first(), bytes.last()),
        (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']'))
    )
}

fn refine(value: &mut Bson) {
    if let Bson::String(text) = &*value {
        let inferred = infer_text(text);
        if !matches!(inferred, Bson::String(_)) {
            *value = inferred;
            return;
        }
    }
    if let Bson::Int64(number) = *value {
        if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&number) {
            *value = Bson::Int32(number as i32);
        }
        return;
    }
    match value {
        Bson::Document(document) => {
            let keys: Vec<String> = document.keys().cloned().collect();
            for key in keys {
                if let Some(nested) = document.get_mut(&key) {
                    refine(nested);
                }
            }
        }
        Bson::Array(items) => {
            for nested in items {
                refine(nested);
            }
        }
        _ => {}
    }
}

/// `pg_dump` writes `2021-04-12 03:00:00+00` and `2021-04-12`. The result
/// is UTC milliseconds, which is what BSON datetime stores.
fn parse_sql_datetime(text: &str) -> Result<Bson, String> {
    let text = text.trim();
    let bad = || format!("bad datetime {text}");
    if text.len() < 10 || text.as_bytes().get(4) != Some(&b'-') || text.as_bytes().get(7) != Some(&b'-')
    {
        return Err(bad());
    }
    let year: i32 = text[0..4].parse().map_err(|_| bad())?;
    let month: u32 = text[5..7].parse().map_err(|_| bad())?;
    let day: u32 = text[8..10].parse().map_err(|_| bad())?;
    let rest = text[10..].trim();
    let rest = rest
        .strip_prefix('T')
        .or_else(|| rest.strip_prefix('t'))
        .unwrap_or(rest);
    let (hour, minute, second, millis, offset_minutes) = if rest.is_empty() {
        (0, 0, 0, 0, 0)
    } else {
        parse_clock(rest).map_err(|_| bad())?
    };
    let days = civil_days(year, month, day).map_err(|_| bad())?;
    let local_ms = days * 86_400_000
        + i64::from(hour) * 3_600_000
        + i64::from(minute) * 60_000
        + i64::from(second) * 1000
        + i64::from(millis);
    let utc_ms = local_ms - i64::from(offset_minutes) * 60_000;
    Ok(Bson::DateTime(bson::DateTime::from_millis(utc_ms)))
}

fn parse_clock(rest: &str) -> Result<(u32, u32, u32, i32, i32), ()> {
    if rest.len() < 8 || rest.as_bytes()[2] != b':' || rest.as_bytes()[5] != b':' {
        return Err(());
    }
    let hour: u32 = rest[0..2].parse().map_err(|_| ())?;
    let minute: u32 = rest[3..5].parse().map_err(|_| ())?;
    let second: u32 = rest[6..8].parse().map_err(|_| ())?;
    if hour > 23 || minute > 59 || second > 60 {
        return Err(());
    }
    let bytes = rest.as_bytes();
    let mut index = 8;
    let millis = if index < bytes.len() && bytes[index] == b'.' {
        index += 1;
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        let fraction = &rest[start..index];
        if fraction.is_empty() {
            return Err(());
        }
        match fraction.len() {
            1 => fraction.parse::<i32>().map_err(|_| ())? * 100,
            2 => fraction.parse::<i32>().map_err(|_| ())? * 10,
            _ => fraction[..3].parse::<i32>().map_err(|_| ())?,
        }
    } else {
        0
    };
    let offset_minutes = parse_offset(rest[index..].trim())?;
    Ok((hour, minute, second, millis, offset_minutes))
}

fn parse_offset(text: &str) -> Result<i32, ()> {
    if text.is_empty() || text == "Z" || text.eq_ignore_ascii_case("UTC") {
        return Ok(0);
    }
    let sign: i32 = match text.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Err(()),
    };
    let body = &text[1..];
    let (hours, minutes) = if let Some((hours, rest)) = body.split_once(':') {
        let hours: u32 = hours.parse().map_err(|_| ())?;
        let minutes: u32 = rest
            .split(':')
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| ())?;
        (hours, minutes)
    } else if body.len() == 2 {
        (body.parse().map_err(|_| ())?, 0)
    } else if body.len() == 4 {
        (
            body[..2].parse().map_err(|_| ())?,
            body[2..].parse().map_err(|_| ())?,
        )
    } else {
        return Err(());
    };
    if hours > 23 || minutes > 59 {
        return Err(());
    }
    Ok(sign * (hours as i32 * 60 + minutes as i32))
}

/// Days from 1970-01-01. Howard Hinnant's civil-from-days conversion.
fn civil_days(year: i32, month: u32, day: u32) -> Result<i64, ()> {
    if !(1..=12).contains(&month) || day == 0 || day > 31 {
        return Err(());
    }
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u32;
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok(i64::from(era) * 146097 + i64::from(day_of_era) - 719468)
}

fn parse_bool(token: &str) -> Result<Bson, String> {
    match token.to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "yes" => Ok(Bson::Boolean(true)),
        "f" | "false" | "0" | "no" => Ok(Bson::Boolean(false)),
        _ => Err(format!("bad boolean {token}")),
    }
}

fn is_word_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_word_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

fn floor_char(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_copy_and_insert_become_documents() {
        let text = r#"
--
-- PostgreSQL database dump
--
\restrict token
SET statement_timeout = 0;
CREATE FUNCTION keep() RETURNS void AS $$ BEGIN PERFORM 1; END $$ LANGUAGE plpgsql;
CREATE TABLE public.emp (
    empno integer NOT NULL,
    ename text,
    sal numeric(10,2),
    hired boolean
);
COPY public.emp (empno, ename, sal, hired) FROM stdin;
7369	SMITH	800.50	t
7499	ALLEN	\N	f
\.
CREATE TABLE public.dept (
    deptno integer,
    dname text
);
INSERT INTO public.dept (deptno, dname) VALUES (10, 'ACCOUNTING');
INSERT INTO public.dept VALUES (20, 'RESEARCH');
\unrestrict token
"#;
        assert!(matches!(detect(text), Ok(Dialect::Postgres)));
        let dump = parse(text, Dialect::Postgres).expect("parse");
        assert_eq!(dump.collections[0].name, "emp");
        assert_eq!(dump.collections[0].documents.len(), 2);
        assert_eq!(
            dump.collections[0].documents[0].get_str("ename"),
            Ok("SMITH")
        );
        assert_eq!(dump.collections[0].documents[0].get_i32("empno"), Ok(7369));
        assert_eq!(dump.collections[0].documents[0].get_f64("sal"), Ok(800.50));
        assert_eq!(dump.collections[0].documents[0].get_bool("hired"), Ok(true));
        assert_eq!(
            dump.collections[0].documents[1].get("sal"),
            Some(&Bson::Null)
        );
        assert_eq!(
            dump.collections[1].documents[1].get_str("dname"),
            Ok("RESEARCH")
        );
    }

    #[test]
    fn json_and_timestamps_keep_their_types() {
        let text = r#"
--
-- PostgreSQL database dump
--
CREATE TABLE public.proposta_venda (
    id integer NOT NULL,
    data_inicio_juros timestamp with time zone,
    quando timestamp(6) without time zone,
    dia date,
    parcelas json
);
INSERT INTO public.proposta_venda VALUES (
    1,
    '2021-04-12 03:00:00+00',
    '2018-02-22 14:21:05.739776',
    '2021-04-12',
    '{"parcelas":[{"indice":1,"valor_parcela":500,"data_ultima_parcela":"2021-04-28T03:00:00.000Z","tipo_parcela":"MENSAL"}]}'
);
INSERT INTO public.proposta_venda VALUES (
    2,
    '2021-04-12 03:00:00-03',
    NULL,
    NULL,
    NULL
);
"#;
        let dump = parse(text, Dialect::Postgres).expect("parse");
        let first = &dump.collections[0].documents[0];
        assert_eq!(
            first.get_datetime("data_inicio_juros").unwrap().timestamp_millis(),
            1_618_196_400_000
        );
        assert_eq!(
            first.get_datetime("quando").unwrap().timestamp_millis(),
            1_519_309_265_739
        );
        assert_eq!(
            first.get_datetime("dia").unwrap().timestamp_millis(),
            1_618_185_600_000
        );
        let parcelas = first.get_document("parcelas").expect("document");
        let row = parcelas.get_array("parcelas").unwrap()[0]
            .as_document()
            .unwrap();
        assert_eq!(row.get_i32("indice"), Ok(1));
        assert_eq!(row.get_i32("valor_parcela"), Ok(500));
        assert_eq!(row.get_str("tipo_parcela"), Ok("MENSAL"));
        assert_eq!(
            row.get_datetime("data_ultima_parcela")
                .unwrap()
                .timestamp_millis(),
            1_619_578_800_000
        );
        let second = &dump.collections[0].documents[1];
        assert_eq!(
            second
                .get_datetime("data_inicio_juros")
                .unwrap()
                .timestamp_millis(),
            1_618_207_200_000
        );
        assert_eq!(second.get("parcelas"), Some(&Bson::Null));
    }

    #[test]
    fn a_value_is_read_from_its_shape() {
        let text = r#"
--
-- PostgreSQL database dump
--
CREATE TABLE public.amostra (
    nota text,
    valor text,
    codigo text,
    flag text,
    corpo json
);
INSERT INTO public.amostra VALUES (
    '2021-05-07T03:00:00.000Z',
    '50000.00',
    '00004345951',
    'true',
    '{"Vencimento":"2021-05-07T03:00:00.000Z","Valor":120,"valor_limite":"50000.00","NossoNumero":"00004345951","ativo":"false","nome":"ARBORE"}'
);
"#;
        let dump = parse(text, Dialect::Postgres).expect("parse");
        let row = &dump.collections[0].documents[0];
        assert_eq!(
            row.get_datetime("nota").unwrap().timestamp_millis(),
            1_620_356_400_000
        );
        assert_eq!(row.get_f64("valor"), Ok(50000.0));
        assert_eq!(row.get_str("codigo"), Ok("00004345951"));
        assert_eq!(row.get_bool("flag"), Ok(true));
        let body = row.get_document("corpo").unwrap();
        assert_eq!(
            body.get_datetime("Vencimento").unwrap().timestamp_millis(),
            1_620_356_400_000
        );
        assert_eq!(body.get_i32("Valor"), Ok(120));
        assert_eq!(body.get_f64("valor_limite"), Ok(50000.0));
        assert_eq!(body.get_str("NossoNumero"), Ok("00004345951"));
        assert_eq!(body.get_bool("ativo"), Ok(false));
        assert_eq!(body.get_str("nome"), Ok("ARBORE"));
    }

    #[test]
    fn postgres_arrays_become_bson_arrays() {
        let text = r#"
--
-- PostgreSQL database dump
--
CREATE TABLE public.usuario (
    id integer NOT NULL,
    empresas_acesso integer[],
    acesso text[]
);
INSERT INTO public.usuario VALUES (1, '{1,2,3,4,5}', '{OPERACOES_VENDAS,RELATORIOS}');
INSERT INTO public.usuario VALUES (2, '{}', '{"a b","c\"d"}');
INSERT INTO public.usuario VALUES (3, '{NULL,9}', NULL);
"#;
        let dump = parse(text, Dialect::Postgres).expect("parse");
        let rows = &dump.collections[0].documents;
        let ids = rows[0].get_array("empresas_acesso").unwrap();
        assert_eq!(
            ids.iter().map(|value| value.as_i32().unwrap()).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        let labels = rows[0].get_array("acesso").unwrap();
        assert_eq!(labels[0].as_str(), Some("OPERACOES_VENDAS"));
        assert_eq!(labels[1].as_str(), Some("RELATORIOS"));
        assert_eq!(rows[1].get_array("empresas_acesso").unwrap().len(), 0);
        let quoted = rows[1].get_array("acesso").unwrap();
        assert_eq!(quoted[0].as_str(), Some("a b"));
        assert_eq!(quoted[1].as_str(), Some("c\"d"));
        assert_eq!(rows[2].get_array("empresas_acesso").unwrap()[0], Bson::Null);
        assert_eq!(rows[2].get("acesso"), Some(&Bson::Null));
    }

    #[test]
    fn mysql_extended_insert_becomes_documents() {
        let text = r#"
-- MySQL dump 10.13  Distrib 8.0.36
CREATE TABLE `bonus` (
  `ename` varchar(10) DEFAULT NULL,
  `job` varchar(9) DEFAULT NULL,
  `sal` int DEFAULT NULL,
  PRIMARY KEY (`ename`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
LOCK TABLES `bonus` WRITE;
INSERT INTO `bonus` VALUES ('SMITH','CLERK',800),('O\'Brien','SALESMAN',1600);
UNLOCK TABLES;
"#;
        assert!(matches!(detect(text), Ok(Dialect::Mysql)));
        let dump = parse(text, Dialect::Mysql).expect("parse");
        assert_eq!(dump.collections[0].name, "bonus");
        assert_eq!(dump.collections[0].documents.len(), 2);
        assert_eq!(dump.collections[0].documents[0].get_i32("sal"), Ok(800));
        assert_eq!(
            dump.collections[0].documents[1].get_str("ename"),
            Ok("O'Brien")
        );
    }
}
