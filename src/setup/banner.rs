//! Cover from the home page.
//!
//! The K is the same six rectangles as the page symbol, sampled on a
//! 48×48 grid so the bars stay four times the gap. Color follows the
//! palette when stdout is a terminal and `NO_COLOR` is unset. The
//! wordmark is the terminal face: a block letter would throw away the
//! resolution this grid just gained.

use std::io::{self, IsTerminal};

const MARK: u32 = 48;

const GRANITO: Rgb = Rgb(23, 25, 28);
const STONE: Rgb = Rgb(236, 238, 235);
const OXIDE: Rgb = Rgb(194, 70, 27);
const MUTED: Rgb = Rgb(154, 161, 166);
const LEAD: Rgb = Rgb(211, 215, 214);

#[derive(Clone, Copy, PartialEq, Eq)]
struct Rgb(u8, u8, u8);

#[derive(Clone, Copy, PartialEq, Eq)]
struct Cell {
    ch: char,
    ink: Option<Ink>,
    bold: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ink {
    Stone,
    Oxide,
    Muted,
    Lead,
}

/// Prints the cover. Plain text when the terminal cannot take color.
pub fn print_banner() {
    print!("{}", render(use_color()));
}

fn use_color() -> bool {
    if !io::stdout().is_terminal() {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    !matches!(std::env::var("TERM").as_deref(), Ok("dumb"))
}

pub(crate) fn render(color: bool) -> String {
    let mut out = String::new();
    for line in plate() {
        out.push_str(&paint(&line, color));
        out.push('\n');
    }
    out
}

fn plate() -> Vec<Vec<Cell>> {
    let width = MARK as usize;
    let mut lines = Vec::new();
    lines.push(pad(
        runs(&[(Ink::Oxide, "████"), (Ink::Muted, "  DATABASE ENGINE")]),
        width,
    ));
    lines.push(blank(width));
    lines.extend(mark());
    lines.push(blank(width));
    lines.push(pad(runs(&[(Ink::Stone, "KOVA")]).bold(), width));
    lines.push(pad(
        runs(&[(Ink::Lead, "Documents, SQL, and text search.")]),
        width,
    ));
    lines
}

fn mark() -> Vec<Vec<Cell>> {
    (0..MARK)
        .step_by(2)
        .map(|py| {
            (0..MARK)
                .map(|px| match sample(px, py) {
                    Some(ink) => cell('█', ink),
                    None => cell(' ', Ink::Stone),
                })
                .collect()
        })
        .collect()
}

/// `true` when the pixel sits on the oxide core.
fn sample(px: u32, py: u32) -> Option<Ink> {
    // One pixel is 2.5 units of the 120×120 symbol.
    let x = (px as f32 + 0.5) * 2.5;
    let y = (py as f32 + 0.5) * 2.5;
    const SLABS: [(f32, f32, f32, f32, bool); 6] = [
        (0.0, 0.0, 20.0, 120.0, false),
        (70.0, 0.0, 50.0, 20.0, false),
        (50.0, 25.0, 50.0, 20.0, false),
        (30.0, 50.0, 50.0, 20.0, true),
        (50.0, 75.0, 50.0, 20.0, false),
        (70.0, 100.0, 50.0, 20.0, false),
    ];
    SLABS.into_iter().find_map(|(sx, sy, w, h, oxide)| {
        (sx <= x && x < sx + w && sy <= y && y < sy + h).then_some(if oxide {
            Ink::Oxide
        } else {
            Ink::Stone
        })
    })
}

fn cell(ch: char, ink: Ink) -> Cell {
    Cell {
        ch,
        ink: if ch == ' ' { None } else { Some(ink) },
        bold: false,
    }
}

struct Run(Vec<Cell>);

impl Run {
    fn bold(mut self) -> Self {
        for cell in &mut self.0 {
            cell.bold = true;
        }
        self
    }
}

fn runs(parts: &[(Ink, &str)]) -> Run {
    Run(parts
        .iter()
        .flat_map(|(ink, text)| text.chars().map(|ch| cell(ch, *ink)))
        .collect())
}

fn pad(run: Run, width: usize) -> Vec<Cell> {
    let mut line = run.0;
    line.resize(width, cell(' ', Ink::Stone));
    line
}

fn blank(width: usize) -> Vec<Cell> {
    vec![cell(' ', Ink::Stone); width]
}

fn paint(line: &[Cell], color: bool) -> String {
    if !color {
        return line.iter().map(|cell| cell.ch).collect();
    }
    let mut out = format!("\x1b[48;2;{};{};{}m", GRANITO.0, GRANITO.1, GRANITO.2);
    let mut index = 0;
    while index < line.len() {
        let ink = line[index].ink;
        let bold = line[index].bold;
        let mut end = index + 1;
        while end < line.len() && line[end].ink == ink && line[end].bold == bold {
            end += 1;
        }
        if let Some(ink) = ink {
            let Rgb(red, green, blue) = rgb(ink);
            out.push_str(&format!("\x1b[38;2;{red};{green};{blue}m"));
            out.push_str(if bold { "\x1b[1m" } else { "\x1b[22m" });
        }
        for cell in &line[index..end] {
            out.push(cell.ch);
        }
        index = end;
    }
    out.push_str("\x1b[0m");
    out
}

fn rgb(ink: Ink) -> Rgb {
    match ink {
        Ink::Stone => STONE,
        Ink::Oxide => OXIDE,
        Ink::Muted => MUTED,
        Ink::Lead => LEAD,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_mark() -> Vec<String> {
        mark()
            .into_iter()
            .map(|line| line.iter().map(|cell| cell.ch).collect())
            .collect()
    }

    #[test]
    fn mark_uses_the_symbol_proportions() {
        let rows = plain_mark();
        assert_eq!(rows.len(), 24);
        assert!(rows.iter().all(|row| row.chars().count() == 48));
        // Spine, a gap the width of the spine, then the top slab.
        assert!(rows[0].starts_with("████████                    ████████████████████"));
        // The gap between slabs is one row, and only the spine remains.
        assert_eq!(rows[4].chars().filter(|ch| *ch == '█').count(), 8);
        // The core sits closer to the spine than the outer slabs.
        assert!(rows[11].starts_with("████████    ████████████████████"));
        assert_eq!(rows[11].chars().filter(|ch| *ch == '█').count(), 28);
    }

    #[test]
    fn color_uses_the_page_palette_and_plain_text_does_not() {
        let plain = render(false);
        let colored = render(true);
        assert!(!plain.contains('\u{1b}'));
        assert!(plain.contains("DATABASE ENGINE"));
        assert!(plain.contains("KOVA"));
        assert!(plain.contains("Documents, SQL, and text search."));
        assert!(colored.contains("\x1b[48;2;23;25;28m"));
        assert!(colored.contains("\x1b[38;2;194;70;27m"));
        assert!(colored.contains("\x1b[38;2;236;238;235m"));
        assert!(colored.contains("\x1b[38;2;154;161;166m"));
        assert!(colored.contains("\x1b[38;2;211;215;214m"));
    }
}
