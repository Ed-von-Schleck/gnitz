//! The output options `\pset` shows and sets, and how each is spelled: the
//! spellings of an enumerated option are its clap value names, the ones `-F`
//! takes on the command line.

use clap::ValueEnum;
use gnitz_sql::sqlparser::tokenizer::Token;

use crate::lex::Lexeme;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// A bordered table, aligned per column.
    Aligned,
    /// One line per row, cells joined by `|`.
    Unaligned,
    Csv,
    /// A JSON array, one object per row.
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Expanded {
    Off,
    On,
    /// Expanded only where the aligned table would be wider than the terminal.
    Auto,
}

/// How results are printed: one field per [`Setting`].
#[derive(Clone, Debug)]
pub struct Settings {
    pub format: Format,
    pub expanded: Expanded,
    /// How a NULL cell is spelled.
    pub null: String,
    /// Whether a table ends with its row count.
    pub footer: bool,
    /// Whether output to a terminal too long or wide for it goes to the pager.
    pub pager: bool,
    /// Whether each statement reports how long it took.
    pub timing: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            format: Format::Aligned,
            expanded: Expanded::Off,
            null: "NULL".to_string(),
            footer: true,
            pager: true,
            timing: false,
        }
    }
}

/// An option `\pset` shows and sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Setting {
    Format,
    Null,
    Footer,
    Pager,
    Expanded,
    Timing,
}

impl Setting {
    /// What its status line calls it: "Output format is csv."
    pub fn label(self) -> &'static str {
        match self {
            Setting::Format => "Output format",
            Setting::Null => "Null display",
            Setting::Footer => "Footer",
            Setting::Pager => "Pager",
            Setting::Expanded => "Expanded display",
            Setting::Timing => "Timing",
        }
    }

    /// The values completion offers.
    pub fn values(self) -> Vec<String> {
        match self {
            Setting::Format => variants::<Format>(),
            Setting::Expanded => variants::<Expanded>(),
            Setting::Footer | Setting::Pager | Setting::Timing => vec![on_off(true).into(), on_off(false).into()],
            Setting::Null => Vec::new(),
        }
    }

    pub fn show(self, s: &Settings) -> String {
        match self {
            Setting::Format => name_of(&s.format),
            Setting::Null => format!("\"{}\"", s.null),
            Setting::Footer => on_off(s.footer).to_string(),
            Setting::Pager => on_off(s.pager).to_string(),
            Setting::Expanded => name_of(&s.expanded),
            Setting::Timing => on_off(s.timing).to_string(),
        }
    }

    /// Apply `value`. `None` toggles `Footer`, `Pager`, `Timing` and `Expanded`
    /// (`Off`↔`On`), and leaves `Format` and `Null` as they are.
    pub fn set(self, s: &mut Settings, value: Option<&str>) -> Result<(), String> {
        let flag = |current: bool| match value {
            None => Ok(!current),
            Some(v) => gnitz_sql::parse_bool(v).ok_or_else(|| format!("\"{v}\" is not on or off")),
        };
        match self {
            Setting::Format => {
                if let Some(v) = value {
                    s.format = choose(v)?;
                }
            }
            Setting::Null => {
                if let Some(v) = value {
                    s.null = single_quoted(v).unwrap_or_else(|| v.to_string());
                }
            }
            Setting::Footer => s.footer = flag(s.footer)?,
            Setting::Pager => s.pager = flag(s.pager)?,
            Setting::Timing => s.timing = flag(s.timing)?,
            Setting::Expanded => {
                s.expanded = match value {
                    None if s.expanded == Expanded::Off => Expanded::On,
                    None => Expanded::Off,
                    Some(v) => match gnitz_sql::parse_bool(v) {
                        Some(true) => Expanded::On,
                        Some(false) => Expanded::Off,
                        None => choose(v)?,
                    },
                }
            }
        }
        Ok(())
    }
}

/// How a variant is spelled.
pub fn name_of(v: &impl ValueEnum) -> String {
    v.to_possible_value()
        .expect("no variant is skipped")
        .get_name()
        .to_string()
}

/// Every variant's spelling.
pub fn variants<E: ValueEnum>() -> Vec<String> {
    E::value_variants().iter().map(name_of).collect()
}

/// The variant `v` spells, in any case; the error lists [`variants`].
pub fn choose<E: ValueEnum>(v: &str) -> Result<E, String> {
    E::from_str(v, true).map_err(|_| format!("\"{v}\" is not one of {}", variants::<E>().join(", ")))
}

fn on_off(b: bool) -> &'static str {
    match b {
        true => "on",
        false => "off",
    }
}

/// The text of `v` when it lexes as one single-quoted string.
fn single_quoted(v: &str) -> Option<String> {
    match crate::lex::lex(v)?.as_slice() {
        [
            Lexeme {
                token: Token::SingleQuotedString(text), ..
            },
        ] => Some(text.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/settings.rs"]
mod tests;
