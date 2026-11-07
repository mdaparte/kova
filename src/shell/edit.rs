//! Line editing for the shell. A pipe reads lines. A terminal handles
//! the up arrow and Tab. Tab completes a collection name after `use`
//! has selected a database and the line is `db.` plus the start of a name.

use std::io::{self, BufRead, IsTerminal, Write};
use std::sync::{Arc, Mutex};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

pub struct Recall {
    past: Vec<String>,
    /// `past.len()` is the draft line, the one being typed.
    index: usize,
    draft: String,
}

impl Default for Recall {
    fn default() -> Self {
        Self::new()
    }
}

impl Recall {
    pub fn new() -> Self {
        Self {
            past: Vec::new(),
            index: 0,
            draft: String::new(),
        }
    }

    pub fn push(&mut self, line: String) {
        if !line.is_empty() && self.past.last() != Some(&line) {
            self.past.push(line);
        }
        self.index = self.past.len();
        self.draft.clear();
    }

    pub fn up(&mut self, current: &str) -> Option<String> {
        if self.past.is_empty() {
            return None;
        }
        if self.index == self.past.len() {
            self.draft = current.to_string();
        }
        if self.index > 0 {
            self.index -= 1;
        }
        Some(self.past[self.index].clone())
    }

    pub fn down(&mut self, _current: &str) -> Option<String> {
        if self.index >= self.past.len() {
            return None;
        }
        self.index += 1;
        if self.index == self.past.len() {
            Some(std::mem::take(&mut self.draft))
        } else {
            Some(self.past[self.index].clone())
        }
    }
}

/// Where a collection name can be completed, and the names that match.
pub fn complete_collection(
    line: &str,
    cursor: usize,
    names: &[String],
) -> Option<(usize, Vec<String>)> {
    let head = line.get(..cursor)?;
    let partial = head.strip_prefix("db.")?;
    if partial.contains('(') || partial.contains('.') || partial.contains(' ') {
        return None;
    }
    let matches: Vec<String> = names
        .iter()
        .filter(|name| name.starts_with(partial))
        .cloned()
        .collect();
    if matches.is_empty() {
        None
    } else {
        Some((3, matches))
    }
}

pub fn read_loop(
    prompt: impl Fn() -> String,
    collections: Arc<Mutex<Vec<String>>>,
    mut on_line: impl FnMut(String),
) -> Result<(), String> {
    if io::stdin().is_terminal() {
        read_terminal(prompt, collections, on_line)
    } else {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line.map_err(|err| format!("read failed: {err}"))?;
            on_line(line);
        }
        Ok(())
    }
}

fn read_terminal(
    prompt: impl Fn() -> String,
    collections: Arc<Mutex<Vec<String>>>,
    mut on_line: impl FnMut(String),
) -> Result<(), String> {
    enable_raw_mode().map_err(|err| format!("terminal: {err}"))?;
    let _restore = Restore;
    let mut buffer = String::new();
    let mut recall = Recall::new();
    loop {
        draw(&prompt(), &buffer)?;
        let event = event::read().map_err(|err| format!("read failed: {err}"))?;
        let Event::Key(key) = event else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        match key.code {
            KeyCode::Enter => {
                eprint!("\r\n");
                let _ = io::stderr().flush();
                let line = std::mem::take(&mut buffer);
                recall.push(line.clone());
                let _ = disable_raw_mode();
                on_line(line);
                enable_raw_mode().map_err(|err| format!("terminal: {err}"))?;
            }
            KeyCode::Char('d')
                if key.modifiers.contains(KeyModifiers::CONTROL) && buffer.is_empty() =>
            {
                eprint!("\r\n");
                return Ok(());
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                eprint!("\r\n");
                return Ok(());
            }
            KeyCode::Char(ch) => buffer.push(ch),
            KeyCode::Backspace => {
                buffer.pop();
            }
            KeyCode::Up => {
                if let Some(line) = recall.up(&buffer) {
                    buffer = line;
                }
            }
            KeyCode::Down => {
                if let Some(line) = recall.down(&buffer) {
                    buffer = line;
                }
            }
            KeyCode::Tab => {
                let names = collections.lock().expect("collection names").clone();
                if let Some((start, matches)) = complete_collection(&buffer, buffer.len(), &names) {
                    if matches.len() == 1 {
                        buffer.truncate(start);
                        buffer.push_str(&matches[0]);
                    } else {
                        eprint!("\r\n{}\r\n", matches.join("  "));
                    }
                }
            }
            _ => {}
        }
    }
}

fn draw(prompt: &str, buffer: &str) -> Result<(), String> {
    eprint!("\r\x1b[2K{prompt}{buffer}");
    io::stderr()
        .flush()
        .map_err(|err| format!("write failed: {err}"))
}

struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}
