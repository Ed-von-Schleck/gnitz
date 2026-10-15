//! Backslash meta-commands, after psql's: the one table that `\?`, completion,
//! highlighting and dispatch all read, and the commands themselves.

use std::fmt::Write as _;
use std::ops::Range;
use std::path::PathBuf;
use std::process::Command;

use clap::ValueEnum;
use gnitz_core::{Catalog, CatalogRelation, RelProps};
use gnitz_expr::{LikeMatcher, LikePattern};
use gnitz_wire::sys_rows::FkAction;
use gnitz_wire::{RelClass, TypeCode};

use crate::render::{choose, name_of, render, variants, Expanded, Format, Settings, Weights};
use crate::shell::{Failed, Flow, Shell};
use crate::value::{Rows, Value};

/// What a meta-command's argument at one position is, for completion.
#[derive(Clone, Copy, Debug)]
pub enum ArgKind {
    None,
    Relation,
    Index,
    Schema,
    /// A view name or a `SELECT` over one.
    Sql,
    /// A [`Setting`]'s name.
    SettingName,
    /// A value of that setting, or of the one the first argument names.
    Value(Option<Setting>),
    Path,
}

/// One meta-command line, as its handler reads it.
pub struct Call<'a> {
    /// Everything after the command word, trimmed.
    pub rest: &'a str,
    /// Whether the command word ended in `+`.
    pub verbose: bool,
}

impl Call<'_> {
    /// The first argument.
    pub fn arg(&self) -> Option<&str> {
        self.rest.split_whitespace().next()
    }
}

pub struct MetaCommand {
    /// The command's name, then its aliases.
    pub names: &'static [&'static str],
    /// The argument synopsis `\?` prints.
    pub args: &'static str,
    pub help: &'static str,
    /// The kind of each positional argument.
    pub arg_kinds: &'static [ArgKind],
    /// What the command does.
    pub run: fn(&mut Shell, &Call<'_>) -> Result<Flow, Failed>,
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

pub const COMMANDS: &[MetaCommand] = &[
    MetaCommand {
        names: &["\\?", "\\h", "\\help"],
        args: "",
        help: "show this help",
        arg_kinds: &[],
        run: |sh, _| sh.help(),
    },
    MetaCommand {
        names: &["\\q", "\\quit"],
        args: "",
        help: "quit",
        arg_kinds: &[],
        run: |_, _| Ok(Flow::Quit),
    },
    MetaCommand {
        names: &["\\c"],
        args: "[TARGET]",
        help: "reconnect, to TARGET if given",
        arg_kinds: &[ArgKind::Path],
        run: |sh, c| sh.connect_to(c.arg()),
    },
    MetaCommand {
        names: &["\\conninfo"],
        args: "",
        help: "show the connection",
        arg_kinds: &[],
        run: |sh, _| sh.conninfo(),
    },
    MetaCommand {
        names: &["\\use"],
        args: "SCHEMA",
        help: "resolve unqualified names in SCHEMA",
        arg_kinds: &[ArgKind::Schema],
        run: |sh, c| match c.arg() {
            Some(schema) => sh.use_schema(schema),
            None => sh.fail("\\use needs a schema name"),
        },
    },
    MetaCommand {
        names: &["\\d"],
        args: "[NAME]",
        help: "list relations, or describe NAME (\\d+ shows hidden columns)",
        arg_kinds: &[ArgKind::Relation],
        run: |sh, c| match c.arg() {
            Some(name) => sh.describe(name, c.verbose),
            None => sh.list(None, |k| k != RelClass::Catalog),
        },
    },
    MetaCommand {
        names: &["\\dt"],
        args: "[PATTERN]",
        help: "list tables",
        arg_kinds: &[ArgKind::Relation],
        run: |sh, c| sh.list(c.arg(), |k| k == RelClass::Table),
    },
    MetaCommand {
        names: &["\\dv"],
        args: "[PATTERN]",
        help: "list views",
        arg_kinds: &[ArgKind::Relation],
        run: |sh, c| sh.list(c.arg(), RelClass::is_view),
    },
    MetaCommand {
        names: &["\\ds"],
        args: "[PATTERN]",
        help: "list streams",
        arg_kinds: &[ArgKind::Relation],
        run: |sh, c| sh.list(c.arg(), |k| k == RelClass::Stream),
    },
    MetaCommand {
        names: &["\\dS"],
        args: "[PATTERN]",
        help: "list system relations",
        arg_kinds: &[ArgKind::Relation],
        run: |sh, c| sh.list(c.arg(), |k| k == RelClass::Catalog),
    },
    MetaCommand {
        names: &["\\di"],
        args: "[PATTERN]",
        help: "list indexes",
        arg_kinds: &[ArgKind::Index],
        run: |sh, c| sh.list_indexes(c.arg()),
    },
    MetaCommand {
        names: &["\\dn"],
        args: "",
        help: "list schemas",
        arg_kinds: &[],
        run: |sh, _| sh.list_schemas(),
    },
    MetaCommand {
        names: &["\\watch"],
        args: "VIEW | SELECT ...",
        help: "stream a delta-fed view's changes, weights and all, until Ctrl-C",
        arg_kinds: &[ArgKind::Sql],
        run: |sh, c| match c.rest.is_empty() {
            true => sh.fail("\\watch needs a view name or a SELECT over one"),
            false => sh.watch(c.rest).map(|()| Flow::Continue),
        },
    },
    MetaCommand {
        names: &["\\x"],
        args: "[on|off|auto]",
        help: "toggle expanded output",
        arg_kinds: &[ArgKind::Value(Some(Setting::Expanded))],
        run: |sh, c| sh.set_option(Setting::Expanded, c.arg()),
    },
    MetaCommand {
        names: &["\\a"],
        args: "",
        help: "toggle between aligned and unaligned output",
        arg_kinds: &[],
        run: |sh, _| {
            let to = match sh.settings.format {
                Format::Aligned => "unaligned",
                _ => "aligned",
            };
            sh.set_option(Setting::Format, Some(to))
        },
    },
    MetaCommand {
        names: &["\\pset"],
        args: "[OPTION [VALUE]]",
        help: "show the output options, or show or set one",
        arg_kinds: &[ArgKind::SettingName, ArgKind::Value(None)],
        run: |sh, c| sh.pset(c.rest),
    },
    MetaCommand {
        names: &["\\timing"],
        args: "[on|off]",
        help: "toggle timing of statements",
        arg_kinds: &[ArgKind::Value(Some(Setting::Timing))],
        run: |sh, c| sh.set_option(Setting::Timing, c.arg()),
    },
    MetaCommand {
        names: &["\\o"],
        args: "[FILE]",
        help: "send results to FILE, or back to stdout",
        arg_kinds: &[ArgKind::Path],
        run: |sh, c| {
            match c.arg() {
                Some(path) => sh.output_to_file(path)?,
                None => sh.output_to_stdout(),
            }
            Ok(Flow::Continue)
        },
    },
    MetaCommand {
        names: &["\\i"],
        args: "FILE",
        help: "run the commands in FILE",
        arg_kinds: &[ArgKind::Path],
        run: |sh, c| match c.arg() {
            Some(path) => sh.run_file(path),
            None => sh.fail("\\i needs a file name"),
        },
    },
    MetaCommand {
        names: &["\\e"],
        args: "[FILE]",
        help: "edit the last statement, or FILE, in $EDITOR",
        arg_kinds: &[ArgKind::Path],
        run: |sh, c| sh.edit(c.arg()),
    },
    MetaCommand {
        names: &["\\echo"],
        args: "TEXT",
        help: "print TEXT",
        arg_kinds: &[],
        run: |sh, c| {
            sh.write_result(&format!("{}\n", c.rest));
            Ok(Flow::Continue)
        },
    },
    MetaCommand {
        names: &["\\!"],
        args: "[COMMAND]",
        help: "run COMMAND in a shell",
        arg_kinds: &[],
        run: |sh, c| sh.shell_command(c.rest),
    },
];

/// The command `name`, without a `+` suffix, names.
pub fn lookup(name: &str) -> Option<&'static MetaCommand> {
    COMMANDS.iter().find(|c| c.names.contains(&name))
}

/// A meta-command line: where its command word is, whether it asks for the
/// verbose form, and the command it names.
pub struct MetaLine {
    /// The whole command word, `+` included.
    pub name: Range<usize>,
    /// Whether the command word ended in `+`.
    pub verbose: bool,
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
        let word = &src[lead..end];
        let base = word.strip_suffix('+');
        Some(MetaLine {
            name: lead..end,
            verbose: base.is_some(),
            command: lookup(base.unwrap_or(word)),
        })
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
    fn label(self) -> &'static str {
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

    fn show(self, s: &Settings) -> String {
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
    fn set(self, s: &mut Settings, value: Option<&str>) -> Result<(), String> {
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
                    s.null = v.trim_matches('\'').to_string();
                }
            }
            Setting::Footer => s.footer = flag(s.footer)?,
            Setting::Pager => s.pager = flag(s.pager)?,
            Setting::Timing => s.timing = flag(s.timing)?,
            Setting::Expanded => {
                s.expanded = match value {
                    None if s.expanded == Expanded::Off => Expanded::On,
                    None => Expanded::Off,
                    Some(v) => choose(v)?,
                }
            }
        }
        Ok(())
    }
}

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

/// `pattern`, a psql wildcard pattern, as a case-insensitive matcher.
fn glob_matcher(pattern: Option<&str>) -> Option<LikeMatcher> {
    pattern.map(|p| LikeMatcher::compile(LikePattern::glob(p).as_bytes(), true))
}

impl Shell {
    /// Run one meta-command line.
    pub fn meta(&mut self, line: &str) -> Result<Flow, Failed> {
        let line = line.trim();
        let Some(MetaLine { name, verbose, command }) = MetaLine::parse(line) else {
            return self.fail(format!("{line} is not a meta-command"));
        };
        let Some(cmd) = command else {
            return self.fail(format!("invalid command {}; try \\? for help", &line[name]));
        };
        let call = Call { rest: line[name.end..].trim(), verbose };
        (cmd.run)(self, &call)
    }

    fn help(&mut self) -> Result<Flow, Failed> {
        let entries: Vec<(String, &str)> = COMMANDS
            .iter()
            .map(|c| {
                let synopsis = match c.args.is_empty() {
                    true => c.name().to_string(),
                    false => format!("{} {}", c.name(), c.args),
                };
                (synopsis, c.help)
            })
            .collect();
        let width = entries.iter().map(|(s, _)| s.len()).max().unwrap_or(0);
        let mut out = String::from("General\n  SQL statements end with ';'. Tab completes, Ctrl-R searches history,\n  Alt-Enter inserts a newline, Alt-E opens the buffer in $EDITOR.\n\nMeta-commands\n");
        for (synopsis, help) in &entries {
            let _ = writeln!(out, "  {synopsis:<width$}  {help}");
        }
        self.write_result(&out);
        Ok(Flow::Continue)
    }

    fn connect_to(&mut self, target: Option<&str>) -> Result<Flow, Failed> {
        let target = target.map_or_else(|| self.target().to_string(), str::to_string);
        self.reconnect(&target)?;
        self.status(&format!("connected to {target}"));
        Ok(Flow::Continue)
    }

    fn conninfo(&mut self) -> Result<Flow, Failed> {
        let msg = format!("connected to \"{}\", schema \"{}\"", self.target(), self.schema());
        self.status(&msg);
        Ok(Flow::Continue)
    }

    fn use_schema(&mut self, schema: &str) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let Some(known) = catalog.schemas.iter().find(|s| s.eq_ignore_ascii_case(schema)) else {
            return self.fail(format!("schema \"{schema}\" does not exist"));
        };
        self.set_schema(known);
        Ok(Flow::Continue)
    }

    /// Apply `value` to `setting`, then report what it is.
    fn set_option(&mut self, setting: Setting, value: Option<&str>) -> Result<Flow, Failed> {
        setting
            .set(&mut self.settings, value)
            .or_else(|e| self.fail(format!("{}: {e}", name_of(&setting))))?;
        self.status(&format!("{} is {}.", setting.label(), setting.show(&self.settings)));
        Ok(Flow::Continue)
    }

    /// `\pset`: every option, or the one `rest` names, set to the rest of the
    /// line when there is one.
    fn pset(&mut self, rest: &str) -> Result<Flow, Failed> {
        let Some(option) = rest.split_whitespace().next() else {
            let rows = Setting::value_variants()
                .iter()
                .map(|s| vec![Value::Text(name_of(s)), Value::Text(s.show(&self.settings))]);
            let rows = Rows::of(&[("Option", TypeCode::String), ("Value", TypeCode::String)], rows);
            self.print_rows(&rows);
            return Ok(Flow::Continue);
        };
        let setting = choose::<Setting>(option).or_else(|e| self.fail(format!("\\pset: {e}")))?;
        let value = rest[option.len()..].trim();
        self.set_option(setting, (!value.is_empty()).then_some(value))
    }

    fn list(&mut self, pattern: Option<&str>, keep: fn(RelClass) -> bool) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let matcher = glob_matcher(pattern);
        let qualified = pattern.is_some_and(|p| p.contains('.'));
        let mut folded = Vec::new();
        let rows: Vec<Vec<Value>> = catalog
            .relations
            .iter()
            .filter(|r| keep(r.class()))
            .filter(|r| {
                let Some(m) = &matcher else {
                    return true;
                };
                match qualified {
                    true => m.matches(r.qualified().as_bytes(), &mut folded),
                    false => m.matches(r.name.as_bytes(), &mut folded),
                }
            })
            .map(|r| {
                vec![
                    Value::Text(r.schema.clone()),
                    Value::Text(r.name.clone()),
                    Value::Text(r.class().describe().to_string()),
                    Value::Int(r.columns.iter().filter(|c| !c.def.is_hidden).count() as i128),
                ]
            })
            .collect();
        if rows.is_empty() {
            self.status("Did not find any matching relations.");
            return Ok(Flow::Continue);
        }
        let columns = [
            ("Schema", TypeCode::String),
            ("Name", TypeCode::String),
            ("Type", TypeCode::String),
            ("Columns", TypeCode::I64),
        ];
        self.print_rows(&Rows::of(&columns, rows));
        Ok(Flow::Continue)
    }

    fn list_indexes(&mut self, pattern: Option<&str>) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let matcher = glob_matcher(pattern);
        let mut folded = Vec::new();
        let rows: Vec<Vec<Value>> = catalog
            .relations
            .iter()
            .flat_map(|r| r.indexes.iter().map(move |i| (r, i)))
            .filter(|(_, i)| {
                matcher
                    .as_ref()
                    .is_none_or(|m| m.matches(i.name.as_bytes(), &mut folded))
            })
            .map(|(r, i)| {
                vec![
                    Value::Text(r.schema.clone()),
                    Value::Text(i.name.clone()),
                    Value::Text(r.name.clone()),
                    Value::Text(r.column_names(&i.cols).join(", ")),
                    Value::Bool(i.is_unique),
                ]
            })
            .collect();
        if rows.is_empty() {
            self.status("Did not find any matching indexes.");
            return Ok(Flow::Continue);
        }
        let columns = [
            ("Schema", TypeCode::String),
            ("Name", TypeCode::String),
            ("Table", TypeCode::String),
            ("Columns", TypeCode::String),
            ("Unique", TypeCode::Bool),
        ];
        self.print_rows(&Rows::of(&columns, rows));
        Ok(Flow::Continue)
    }

    fn list_schemas(&mut self) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let rows = catalog.schemas.into_iter().map(|s| vec![Value::Text(s)]);
        self.print_rows(&Rows::of(&[("Name", TypeCode::String)], rows));
        Ok(Flow::Continue)
    }

    fn describe(&mut self, name: &str, verbose: bool) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let Some(rel) = catalog.find(&self.schema(), name) else {
            return self.fail(format!("Did not find any relation named \"{name}\"."));
        };
        let out = self.description(&catalog, rel, verbose);
        self.write_result(&out);
        Ok(Flow::Continue)
    }

    /// What `\d NAME` prints for `rel`: its columns, then its indexes, foreign
    /// keys both ways, the relations it reads and those that read it, and the
    /// `WITH` options it was created with.
    fn description(&self, catalog: &Catalog, rel: &CatalogRelation, verbose: bool) -> String {
        let mut out = format!("{} \"{}\"\n", capitalize(rel.class().describe()), rel.qualified());
        // A table's only hidden columns are the ones it dropped.
        let hidden = match rel.props {
            RelProps::Table(_) => "dropped",
            RelProps::View(_) => "hidden",
        };
        let pk = rel.pk.as_slice();
        let rows = rel
            .columns
            .iter()
            .enumerate()
            .map(|(ci, c)| (ci, &c.def))
            .filter(|(_, c)| verbose || !c.is_hidden)
            .map(|(ci, c)| {
                let mut key = Vec::new();
                match pk.iter().position(|&k| k as usize == ci) {
                    Some(_) if pk.len() == 1 => key.push("PK".to_string()),
                    Some(at) => key.push(format!("PK {}", at + 1)),
                    None => {}
                }
                if c.is_hidden {
                    key.push(hidden.to_string());
                }
                vec![
                    Value::Text(c.name.clone()),
                    Value::Text(gnitz_sql::sql_type_name(c.ty)),
                    Value::Text(if c.is_nullable { "" } else { "not null" }.to_string()),
                    Value::Text(key.join(", ")),
                ]
            });
        let columns = [
            ("Column", TypeCode::String),
            ("Type", TypeCode::String),
            ("Nullable", TypeCode::String),
            ("Key", TypeCode::String),
        ];
        let settings = Settings {
            format: Format::Aligned,
            expanded: Expanded::Off,
            footer: false,
            ..self.settings.clone()
        };
        out.push_str(&render(
            &Rows::of(&columns, rows),
            Weights::Repeat,
            &settings,
            None,
            false,
        ));

        let mut section = |title: &str, lines: Vec<String>| {
            if lines.is_empty() {
                return;
            }
            let _ = writeln!(out, "{title}:");
            for line in lines {
                let _ = writeln!(out, "    {line}");
            }
        };
        let indexes = rel.indexes.iter().map(|i| {
            let unique = if i.is_unique { " UNIQUE" } else { "" };
            format!("\"{}\"{unique} ({})", i.name, rel.column_names(&i.cols).join(", "))
        });
        section("Indexes", indexes.collect());
        let fks = rel.columns.iter().filter_map(|c| {
            let fk = c.fk?;
            let (table, col) = catalog.fk_target(fk)?;
            let on_delete = match fk.on_delete {
                FkAction::Restrict => "",
                FkAction::Cascade => " ON DELETE CASCADE",
            };
            Some(format!(
                "({}) REFERENCES {}({}){on_delete}",
                c.def.name,
                table.qualified(),
                col.def.name
            ))
        });
        section("Foreign keys", fks.collect());
        section(
            "Referenced by",
            catalog.referencing(rel.id).map(CatalogRelation::qualified).collect(),
        );
        section("Reads", catalog.reads_of(rel).map(CatalogRelation::qualified).collect());
        section(
            "Read by",
            catalog.readers(rel.id).map(CatalogRelation::qualified).collect(),
        );
        section("With", gnitz_sql::with_options(&rel.props));
        out
    }

    fn edit(&mut self, file: Option<&str>) -> Result<Flow, Failed> {
        if !self.interactive {
            return self.fail("\\e needs an interactive session");
        }
        let path = match file {
            Some(f) => PathBuf::from(f),
            None => {
                let path = self.scratch()?.join("edit.sql");
                let text = self.last_statement().to_string();
                std::fs::write(&path, text).or_else(|e| self.fail(format!("{}: {e}", path.display())))?;
                path
            }
        };
        match editor_command(&path).status() {
            Ok(s) if s.success() => {}
            Ok(s) => return self.fail(format!("editor exited with {s}")),
            Err(e) => return self.fail(format!("{}: {e}", editor())),
        }
        // Read back by path: an editor may save by renaming a new file over it.
        let text = std::fs::read_to_string(&path).or_else(|e| self.fail(format!("{}: {e}", path.display())))?;
        self.prefill = Some(text.trim_end().to_string());
        Ok(Flow::Continue)
    }

    /// `\!`: `command` through `/bin/sh`, or an interactive `$SHELL`.
    fn shell_command(&mut self, command: &str) -> Result<Flow, Failed> {
        let status = match command.is_empty() {
            true => {
                let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
                Command::new(&shell).status()
            }
            false => Command::new("/bin/sh").arg("-c").arg(command).status(),
        };
        if let Err(e) = status {
            return self.fail(format!("\\!: {e}"));
        }
        Ok(Flow::Continue)
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
