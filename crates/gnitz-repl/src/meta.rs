//! Backslash meta-commands, after psql's: the one table that `\?`, completion,
//! highlighting and dispatch all read, and the commands themselves.

use std::io::Write;
use std::ops::Range;
use std::process::Command;

use clap::ValueEnum;
use gnitz_wire::{ColType, RelClass, TypeCode};

use crate::render::{name_of, render, Expanded, Format, Settings, Weights};
use crate::shell::{Failed, Flow, Shell};
use crate::value::{Column, Rows, Value};

/// What a meta-command's argument at one position is, for completion.
#[derive(Clone, Copy, Debug)]
pub enum ArgKind {
    None,
    Relation,
    Schema,
    /// A view name or a `SELECT` over one.
    Sql,
    Choice(&'static [&'static str]),
    Path,
}

/// What a meta-command does.
#[derive(Clone, Copy)]
enum Action {
    Help,
    Quit,
    Connect,
    ConnInfo,
    Use,
    /// List the relations `keep` admits; `\d` with an argument describes one.
    List {
        keep: fn(RelClass) -> bool,
        describes: bool,
    },
    ListIndexes,
    ListSchemas,
    Watch,
    Expanded,
    ToggleAligned,
    Pset,
    Timing,
    Output,
    Include,
    Edit,
    Echo,
    Shell,
}

pub struct MetaCommand {
    /// The command's name, then its aliases.
    pub names: &'static [&'static str],
    /// The argument synopsis `\?` prints.
    pub args: &'static str,
    pub help: &'static str,
    /// The kind of each positional argument.
    pub arg_kinds: &'static [ArgKind],
    action: Action,
}

impl MetaCommand {
    pub fn name(&self) -> &'static str {
        self.names[0]
    }

    pub fn arg(&self, i: usize) -> ArgKind {
        match (self.arg_kinds.get(i), self.arg_kinds.last()) {
            (Some(&k), _) => k,
            // `\watch`'s SQL runs on past its first word.
            (None, Some(&ArgKind::Sql)) => ArgKind::Sql,
            _ => ArgKind::None,
        }
    }
}

const ON_OFF: &[&str] = &["on", "off"];
const PSET_OPTIONS: &[&str] = &["format", "null", "footer", "pager", "expanded"];

pub const COMMANDS: &[MetaCommand] = &[
    MetaCommand {
        names: &["\\?", "\\h", "\\help"],
        args: "",
        help: "show this help",
        arg_kinds: &[],
        action: Action::Help,
    },
    MetaCommand {
        names: &["\\q", "\\quit"],
        args: "",
        help: "quit",
        arg_kinds: &[],
        action: Action::Quit,
    },
    MetaCommand {
        names: &["\\c"],
        args: "[TARGET]",
        help: "reconnect, to TARGET if given",
        arg_kinds: &[ArgKind::Path],
        action: Action::Connect,
    },
    MetaCommand {
        names: &["\\conninfo"],
        args: "",
        help: "show the connection",
        arg_kinds: &[],
        action: Action::ConnInfo,
    },
    MetaCommand {
        names: &["\\use"],
        args: "SCHEMA",
        help: "resolve unqualified names in SCHEMA",
        arg_kinds: &[ArgKind::Schema],
        action: Action::Use,
    },
    MetaCommand {
        names: &["\\d"],
        args: "[NAME]",
        help: "list relations, or describe NAME (\\d+ shows hidden columns)",
        arg_kinds: &[ArgKind::Relation],
        action: Action::List {
            keep: |c| c != RelClass::Catalog,
            describes: true,
        },
    },
    MetaCommand {
        names: &["\\dt"],
        args: "[PATTERN]",
        help: "list tables",
        arg_kinds: &[ArgKind::Relation],
        action: Action::List {
            keep: |c| c == RelClass::Table,
            describes: false,
        },
    },
    MetaCommand {
        names: &["\\dv"],
        args: "[PATTERN]",
        help: "list views",
        arg_kinds: &[ArgKind::Relation],
        action: Action::List {
            keep: RelClass::is_view,
            describes: false,
        },
    },
    MetaCommand {
        names: &["\\ds"],
        args: "[PATTERN]",
        help: "list streams",
        arg_kinds: &[ArgKind::Relation],
        action: Action::List {
            keep: |c| c == RelClass::Stream,
            describes: false,
        },
    },
    MetaCommand {
        names: &["\\dS"],
        args: "[PATTERN]",
        help: "list system relations",
        arg_kinds: &[ArgKind::Relation],
        action: Action::List {
            keep: |c| c == RelClass::Catalog,
            describes: false,
        },
    },
    MetaCommand {
        names: &["\\di"],
        args: "[PATTERN]",
        help: "list indexes",
        arg_kinds: &[ArgKind::Relation],
        action: Action::ListIndexes,
    },
    MetaCommand {
        names: &["\\dn"],
        args: "",
        help: "list schemas",
        arg_kinds: &[],
        action: Action::ListSchemas,
    },
    MetaCommand {
        names: &["\\watch"],
        args: "VIEW | SELECT ...",
        help: "stream a delta-fed view's changes, weights and all, until Ctrl-C",
        arg_kinds: &[ArgKind::Sql],
        action: Action::Watch,
    },
    MetaCommand {
        names: &["\\x"],
        args: "[on|off|auto]",
        help: "toggle expanded output",
        arg_kinds: &[ArgKind::Choice(&["on", "off", "auto"])],
        action: Action::Expanded,
    },
    MetaCommand {
        names: &["\\a"],
        args: "",
        help: "toggle between aligned and unaligned output",
        arg_kinds: &[],
        action: Action::ToggleAligned,
    },
    MetaCommand {
        names: &["\\pset"],
        args: "[OPTION [VALUE]]",
        help: "show or set an output option (format, null, footer, pager, expanded)",
        arg_kinds: &[
            ArgKind::Choice(PSET_OPTIONS),
            ArgKind::Choice(&["aligned", "unaligned", "csv", "json", "on", "off", "auto"]),
        ],
        action: Action::Pset,
    },
    MetaCommand {
        names: &["\\timing"],
        args: "[on|off]",
        help: "toggle timing of statements",
        arg_kinds: &[ArgKind::Choice(ON_OFF)],
        action: Action::Timing,
    },
    MetaCommand {
        names: &["\\o"],
        args: "[FILE]",
        help: "send results to FILE, or back to stdout",
        arg_kinds: &[ArgKind::Path],
        action: Action::Output,
    },
    MetaCommand {
        names: &["\\i"],
        args: "FILE",
        help: "run the commands in FILE",
        arg_kinds: &[ArgKind::Path],
        action: Action::Include,
    },
    MetaCommand {
        names: &["\\e"],
        args: "[FILE]",
        help: "edit the last statement, or FILE, in $EDITOR",
        arg_kinds: &[ArgKind::Path],
        action: Action::Edit,
    },
    MetaCommand {
        names: &["\\echo"],
        args: "TEXT",
        help: "print TEXT",
        arg_kinds: &[],
        action: Action::Echo,
    },
    MetaCommand {
        names: &["\\!"],
        args: "[COMMAND]",
        help: "run COMMAND in a shell",
        arg_kinds: &[],
        action: Action::Shell,
    },
];

/// The command `name` names; a `+` suffix asks for the verbose form.
pub fn lookup(name: &str) -> Option<&'static MetaCommand> {
    let base = name.strip_suffix('+').unwrap_or(name);
    COMMANDS.iter().find(|c| c.names.contains(&base))
}

/// A meta-command line: where its command word is, and the command it names.
pub struct MetaLine {
    pub name: Range<usize>,
    pub command: Option<&'static MetaCommand>,
}

impl MetaLine {
    /// `src` as a meta-command line, when it is one.
    pub fn parse(src: &str) -> Option<MetaLine> {
        let lead = src.len() - src.trim_start().len();
        if !src[lead..].starts_with('\\') {
            return None;
        }
        let end = src[lead..].find(char::is_whitespace).map_or(src.len(), |i| lead + i);
        Some(MetaLine {
            name: lead..end,
            command: lookup(&src[lead..end]),
        })
    }
}

/// psql's `*` / `?` wildcards, case-insensitively.
pub fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some('*'), _) => go(&p[1..], t) || (!t.is_empty() && go(p, &t[1..])),
            (Some('?'), Some(_)) => go(&p[1..], &t[1..]),
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b) && go(&p[1..], &t[1..]),
            _ => false,
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

fn text_rows(header: &[&str], rows: Vec<Vec<String>>) -> Rows {
    Rows {
        columns: header
            .iter()
            .map(|h| Column {
                name: h.to_string(),
                ty: ColType::of(TypeCode::String),
            })
            .collect(),
        rows: rows
            .into_iter()
            .map(|r| (r.into_iter().map(Value::Text).collect(), 1))
            .collect(),
    }
}

fn parse_on_off(arg: Option<&str>, current: bool) -> Result<bool, String> {
    match arg {
        None => Ok(!current),
        Some("on" | "true" | "1") => Ok(true),
        Some("off" | "false" | "0") => Ok(false),
        Some(other) => Err(format!("unrecognized value \"{other}\": expected on or off")),
    }
}

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

impl Shell {
    /// Run one meta-command line.
    pub fn meta(&mut self, line: &str) -> Result<Flow, Failed> {
        let line = line.trim();
        let Some(MetaLine { name, command }) = MetaLine::parse(line) else {
            return self.fail(format!("{line} is not a meta-command"));
        };
        let rest = line[name.end..].trim();
        let name = &line[name];
        let args: Vec<&str> = rest.split_whitespace().collect();
        let arg = args.first().copied();
        let Some(cmd) = command else {
            return self.fail(format!("invalid command {name}; try \\? for help"));
        };
        match cmd.action {
            Action::Help => self.help(),
            Action::Quit => return Ok(Flow::Quit),
            Action::Connect => {
                let target = arg.map_or_else(|| self.target().to_string(), str::to_string);
                self.reconnect(&target)?;
                self.status(&format!("connected to {target}"));
            }
            Action::ConnInfo => {
                let msg = format!("connected to \"{}\", schema \"{}\"", self.target(), self.schema());
                self.status(&msg);
            }
            Action::Use => {
                let Some(schema) = arg else {
                    return self.fail("\\use needs a schema name");
                };
                self.refresh_catalog();
                let known = self.with_catalog(|c| c.schemas.iter().find(|s| s.eq_ignore_ascii_case(schema)).cloned());
                let Some(schema) = known else {
                    return self.fail(format!("schema \"{schema}\" does not exist"));
                };
                self.set_schema(&schema);
            }
            Action::List { keep, describes } => match arg {
                Some(rel) if describes => self.describe(rel, name.ends_with('+'))?,
                _ => self.list(arg, keep),
            },
            Action::ListIndexes => self.list_indexes(arg),
            Action::ListSchemas => {
                self.refresh_catalog();
                let rows = self.with_catalog(|c| c.schemas.iter().map(|s| vec![s.clone()]).collect());
                self.print_listing(&["Name"], rows, true);
            }
            Action::Watch => {
                if rest.is_empty() {
                    return self.fail("\\watch needs a view name or a SELECT over one");
                }
                self.watch(rest)?;
            }
            Action::Expanded => self.set_expanded(arg)?,
            Action::ToggleAligned => {
                let format = match self.settings.format {
                    Format::Aligned => Format::Unaligned,
                    _ => Format::Aligned,
                };
                self.set_format(format);
            }
            Action::Pset => self.pset(&args)?,
            Action::Timing => {
                self.timing = parse_on_off(arg, self.timing).or_else(|e| self.fail(e))?;
                self.status(&format!("Timing is {}.", on_off(self.timing)));
            }
            Action::Output => match arg {
                Some(path) => self.output_to_file(path)?,
                None => self.output_to_stdout(),
            },
            Action::Include => {
                let Some(path) = arg else {
                    return self.fail("\\i needs a file name");
                };
                let file = std::fs::File::open(path).or_else(|e| self.fail(format!("{path}: {e}")))?;
                return self.run_input(std::io::BufReader::new(file));
            }
            Action::Edit => self.edit(arg)?,
            Action::Echo => self.write_result(&format!("{rest}\n")),
            Action::Shell => {
                let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
                let status = match rest.is_empty() {
                    true => Command::new(&shell).status(),
                    false => Command::new("/bin/sh").arg("-c").arg(rest).status(),
                };
                if let Err(e) = status {
                    return self.fail(format!("\\!: {e}"));
                }
            }
        }
        Ok(Flow::Continue)
    }

    fn help(&mut self) {
        let width = COMMANDS
            .iter()
            .map(|c| c.name().len() + 1 + c.args.len())
            .max()
            .unwrap_or(0);
        let mut out = String::from("General\n  SQL statements end with ';'. Tab completes, Ctrl-R searches history,\n  Alt-Enter inserts a newline, Alt-E opens the buffer in $EDITOR.\n\nMeta-commands\n");
        for c in COMMANDS {
            let synopsis = match c.args.is_empty() {
                true => c.name().to_string(),
                false => format!("{} {}", c.name(), c.args),
            };
            out.push_str(&format!("  {synopsis:<width$}  {}\n", c.help));
        }
        self.write_result(&out);
    }

    fn set_format(&mut self, format: Format) {
        self.settings.format = format;
        self.status(&format!("Output format is {}.", name_of(&format)));
    }

    /// `\x`: on, off or auto; with no value, toggles.
    fn set_expanded(&mut self, arg: Option<&str>) -> Result<(), Failed> {
        self.settings.expanded = match arg {
            None if self.settings.expanded == Expanded::Off => Expanded::On,
            None => Expanded::Off,
            Some(v) => match Expanded::from_str(v, true) {
                Ok(e) => e,
                Err(_) => return self.fail(format!("unrecognized value \"{v}\": expected on, off or auto")),
            },
        };
        self.status(&format!("Expanded display is {}.", name_of(&self.settings.expanded)));
        Ok(())
    }

    fn pset(&mut self, args: &[&str]) -> Result<(), Failed> {
        let Some(&option) = args.first() else {
            let s = &self.settings;
            let lines = format!(
                "format    {}\nnull      \"{}\"\nfooter    {}\npager     {}\nexpanded  {}\n",
                name_of(&s.format),
                s.null,
                on_off(s.footer),
                on_off(s.pager),
                name_of(&s.expanded)
            );
            self.write_result(&lines);
            return Ok(());
        };
        let value = args.get(1).copied();
        match option {
            "format" => match value.and_then(|v| Format::from_str(v, true).ok()) {
                Some(f) => self.set_format(f),
                None => {
                    let names: Vec<String> = Format::value_variants().iter().map(name_of).collect();
                    return self.fail(format!("\\pset format takes one of: {}", names.join(", ")));
                }
            },
            "null" => {
                self.settings.null = value.unwrap_or("").trim_matches('\'').to_string();
                self.status(&format!("Null display is \"{}\".", self.settings.null));
            }
            "footer" => {
                self.settings.footer = parse_on_off(value, self.settings.footer).or_else(|e| self.fail(e))?;
            }
            "pager" => {
                self.settings.pager = parse_on_off(value, self.settings.pager).or_else(|e| self.fail(e))?;
            }
            "expanded" => self.set_expanded(value)?,
            other => {
                return self.fail(format!(
                    "\\pset: unknown option \"{other}\"; one of: {}",
                    PSET_OPTIONS.join(", ")
                ))
            }
        }
        Ok(())
    }

    /// A catalog listing as an aligned table; `footer` counts its rows.
    fn listing(&self, header: &[&str], rows: Vec<Vec<String>>, footer: bool) -> String {
        let settings = Settings {
            format: Format::Aligned,
            expanded: Expanded::Off,
            footer: footer && self.settings.footer,
            ..self.settings.clone()
        };
        render(&text_rows(header, rows), Weights::Repeat, &settings, None, false)
    }

    fn print_listing(&mut self, header: &[&str], rows: Vec<Vec<String>>, footer: bool) {
        let text = self.listing(header, rows, footer);
        self.write_result(&text);
    }

    fn list(&mut self, pattern: Option<&str>, keep: fn(RelClass) -> bool) {
        self.refresh_catalog();
        let rows = self.with_catalog(|c| {
            c.relations
                .iter()
                .filter(|r| keep(r.class))
                .filter(|r| match pattern {
                    Some(p) if p.contains('.') => glob(p, &r.qualified()),
                    Some(p) => glob(p, &r.name),
                    None => true,
                })
                .map(|r| {
                    vec![
                        r.schema.clone(),
                        r.name.clone(),
                        r.class.describe().to_string(),
                        r.columns.iter().filter(|c| !c.is_hidden).count().to_string(),
                    ]
                })
                .collect::<Vec<_>>()
        });
        if rows.is_empty() {
            self.status("Did not find any matching relations.");
            return;
        }
        self.print_listing(&["Schema", "Name", "Type", "Columns"], rows, true);
    }

    fn list_indexes(&mut self, pattern: Option<&str>) {
        self.refresh_catalog();
        let rows = self.with_catalog(|c| {
            c.relations
                .iter()
                .flat_map(|r| r.indexes.iter().map(move |i| (r, i)))
                .filter(|(_, i)| pattern.is_none_or(|p| glob(p, &i.name)))
                .map(|(r, i)| {
                    vec![
                        r.schema.clone(),
                        i.name.clone(),
                        r.name.clone(),
                        i.columns.join(", "),
                        if i.unique { "yes" } else { "no" }.to_string(),
                    ]
                })
                .collect::<Vec<_>>()
        });
        if rows.is_empty() {
            self.status("Did not find any matching indexes.");
            return;
        }
        self.print_listing(&["Schema", "Name", "Table", "Columns", "Unique"], rows, true);
    }

    fn describe(&mut self, name: &str, verbose: bool) -> Result<(), Failed> {
        self.refresh_catalog();
        let schema = self.schema();
        let Some(rel) = self.with_catalog(|c| c.find(&schema, name).cloned()) else {
            return self.fail(format!("Did not find any relation named \"{name}\"."));
        };
        let mut out = format!("{} \"{}\"\n", capitalize(rel.class.describe()), rel.qualified());
        let rows = rel
            .columns
            .iter()
            .filter(|c| verbose || !c.is_hidden)
            .map(|c| {
                let mut key = Vec::new();
                if rel.pk.contains(&c.name) {
                    key.push("PK");
                }
                if c.is_hidden {
                    key.push("hidden");
                }
                vec![
                    c.name.clone(),
                    gnitz_sql::sql_type_name(c.ty),
                    if c.is_nullable { "" } else { "not null" }.to_string(),
                    key.join(", "),
                ]
            })
            .collect();
        out.push_str(&self.listing(&["Column", "Type", "Nullable", "Key"], rows, false));
        if !rel.pk.is_empty() {
            out.push_str(&format!("Primary key: ({})\n", rel.pk.join(", ")));
        }
        if !rel.indexes.is_empty() {
            out.push_str("Indexes:\n");
            for i in &rel.indexes {
                let unique = if i.unique { " UNIQUE" } else { "" };
                out.push_str(&format!("    \"{}\"{unique} ({})\n", i.name, i.columns.join(", ")));
            }
        }
        if let Some(bytes) = rel.budget.capacity_bytes() {
            out.push_str(&format!("Capacity: {bytes} bytes per worker\n"));
        }
        if let Some(bytes) = rel.budget.delta_bytes() {
            out.push_str(&format!("Delta feed: {bytes} bytes retained\n"));
        }
        self.write_result(&out);
        Ok(())
    }

    fn edit(&mut self, file: Option<&str>) -> Result<(), Failed> {
        if !self.interactive {
            return self.fail("\\e needs an interactive session");
        }
        let (path, temp) = match file {
            Some(f) => (std::path::PathBuf::from(f), false),
            None => {
                let path = std::env::temp_dir().join(format!("gnitz-edit-{}.sql", std::process::id()));
                let mut f = std::fs::File::create(&path).or_else(|e| self.fail(format!("{}: {e}", path.display())))?;
                let _ = f.write_all(self.last_statement().as_bytes());
                (path, true)
            }
        };
        let text = match editor_command(&path).status() {
            Ok(s) if s.success() => std::fs::read_to_string(&path).unwrap_or_default(),
            Ok(s) => return self.fail(format!("editor exited with {s}")),
            Err(e) => return self.fail(format!("{}: {e}", editor())),
        };
        if temp {
            let _ = std::fs::remove_file(&path);
        }
        self.prefill = Some(text.trim_end().to_string());
        Ok(())
    }
}

/// `$VISUAL`, else `$EDITOR`, else `vi`.
fn editor() -> String {
    std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string())
}

/// The user's editor, run on `path` through the shell so `$EDITOR` may carry
/// arguments.
pub fn editor_command(path: &std::path::Path) -> Command {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(format!("{} \"$1\"", editor())).arg("sh").arg(path);
    cmd
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map_or_else(String::new, |f| f.to_uppercase().chain(c).collect())
}

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
