//! Backslash meta-commands, after psql's: the one table that `\?`, completion,
//! highlighting and dispatch all read, and the commands themselves.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::ops::Range;
use std::path::PathBuf;
use std::process::Command;

use clap::ValueEnum;
use gnitz_core::{Catalog, CatalogRelation, RelName, RelProps};
use gnitz_expr::{LikeMatcher, LikePattern};
use gnitz_sql::sqlparser::tokenizer::Token;
use gnitz_wire::sys_rows::FkAction;
use gnitz_wire::{qualified_key, RelClass, TypeCode};

use crate::editor::{editor, editor_command, KEY_HELP};
use crate::render::{render, Expanded, Format, Settings, Weights};
use crate::shell::{Failed, Flow, Shell};
use crate::value::{Rows, Value};

/// What a meta-command's argument is, for completion.
#[derive(Clone, Copy)]
pub enum ArgKind {
    None,
    /// A relation of a class the predicate keeps.
    Relation(fn(RelClass) -> bool),
    Index,
    Schema,
    /// SQL to the end of the line over the relations of the classes the
    /// predicate keeps; a relation name alone as its first word.
    Sql(fn(RelClass) -> bool),
    /// A [`Setting`]'s name, then that setting's value.
    Setting,
    /// A value of that setting.
    Value(Setting),
    Path,
}

/// One meta-command line, as its handler reads it.
pub struct Call<'a> {
    pub command: &'static MetaCommand,
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

    /// The first argument, a path, with a leading `~` expanded.
    pub fn path(&self) -> Option<Cow<'_, str>> {
        self.arg().map(expand_home)
    }

    /// The relation classes the command's argument names.
    pub fn relations(&self) -> fn(RelClass) -> bool {
        match self.command.arg {
            ArgKind::Relation(keep) => keep,
            _ => |_| true,
        }
    }
}

/// `path` with a leading `~` or `~/` read as the home directory; `~user` and a
/// home directory that is unknown or not UTF-8 leave it as it is.
pub(crate) fn expand_home(path: &str) -> Cow<'_, str> {
    let Some(rest) = path.strip_prefix('~').filter(|r| r.is_empty() || r.starts_with('/')) else {
        return Cow::Borrowed(path);
    };
    match dirs::home_dir().as_deref().and_then(std::path::Path::to_str) {
        Some(home) => Cow::Owned(format!("{home}{rest}")),
        None => Cow::Borrowed(path),
    }
}

pub struct MetaCommand {
    /// The command's name, then its aliases.
    pub names: &'static [&'static str],
    /// The argument synopsis `\?` prints.
    pub args: &'static str,
    pub help: &'static str,
    /// What its argument is.
    pub arg: ArgKind,
    /// What the command does.
    pub run: fn(&mut Shell, &Call<'_>) -> Result<Flow, Failed>,
}

impl MetaCommand {
    pub fn name(&self) -> &'static str {
        self.names[0]
    }
}

pub const COMMANDS: &[MetaCommand] = &[
    MetaCommand {
        names: &["\\?", "\\h", "\\help"],
        args: "",
        help: "show this help",
        arg: ArgKind::None,
        run: |sh, _| sh.help(),
    },
    MetaCommand {
        names: &["\\q", "\\quit"],
        args: "",
        help: "quit",
        arg: ArgKind::None,
        run: |_, _| Ok(Flow::Quit),
    },
    MetaCommand {
        names: &["\\c"],
        args: "[TARGET]",
        help: "reconnect, to TARGET if given",
        arg: ArgKind::Path,
        run: |sh, c| sh.connect_to(c.path().as_deref()),
    },
    MetaCommand {
        names: &["\\conninfo"],
        args: "",
        help: "show the connection",
        arg: ArgKind::None,
        run: |sh, _| sh.conninfo(),
    },
    MetaCommand {
        names: &["\\use"],
        args: "SCHEMA",
        help: "resolve unqualified names in SCHEMA",
        arg: ArgKind::Schema,
        run: |sh, c| match c.arg() {
            Some(schema) => sh.use_schema(schema),
            None => sh.fail("\\use needs a schema name"),
        },
    },
    MetaCommand {
        names: &["\\d"],
        args: "[NAME]",
        help: "list relations, or describe NAME (\\d+ shows hidden columns and a view's definition)",
        arg: ArgKind::Relation(|_| true),
        run: |sh, c| match c.arg() {
            Some(name) => sh.describe(name, c.verbose),
            None => sh.list(None, |k| k != RelClass::Catalog),
        },
    },
    MetaCommand {
        names: &["\\dt"],
        args: "[PATTERN]",
        help: "list tables",
        arg: ArgKind::Relation(|k| k == RelClass::Table),
        run: |sh, c| sh.list(c.arg(), c.relations()),
    },
    MetaCommand {
        names: &["\\dv"],
        args: "[PATTERN]",
        help: "list views",
        arg: ArgKind::Relation(RelClass::is_view),
        run: |sh, c| sh.list(c.arg(), c.relations()),
    },
    MetaCommand {
        names: &["\\ds"],
        args: "[PATTERN]",
        help: "list streams",
        arg: ArgKind::Relation(|k| k == RelClass::Stream),
        run: |sh, c| sh.list(c.arg(), c.relations()),
    },
    MetaCommand {
        names: &["\\dS"],
        args: "[PATTERN]",
        help: "list system relations",
        arg: ArgKind::Relation(|k| k == RelClass::Catalog),
        run: |sh, c| sh.list(c.arg(), c.relations()),
    },
    MetaCommand {
        names: &["\\di"],
        args: "[PATTERN]",
        help: "list indexes",
        arg: ArgKind::Index,
        run: |sh, c| sh.list_indexes(c.arg()),
    },
    MetaCommand {
        names: &["\\dn"],
        args: "",
        help: "list schemas",
        arg: ArgKind::None,
        run: |sh, _| sh.list_schemas(),
    },
    MetaCommand {
        names: &["\\watch"],
        args: "VIEW | SELECT ...",
        help: "stream a delta-fed view's changes, weights and all, until Ctrl-C",
        arg: ArgKind::Sql(|k| k == RelClass::FedView),
        run: |sh, c| match c.rest.is_empty() {
            true => sh.fail("\\watch needs a view name or a SELECT over one"),
            false => sh.watch(c.rest).map(|()| Flow::Continue),
        },
    },
    MetaCommand {
        names: &["\\x"],
        args: "[on|off|auto]",
        help: "toggle expanded output",
        arg: ArgKind::Value(Setting::Expanded),
        run: |sh, c| sh.set_option(Setting::Expanded, c.arg()),
    },
    MetaCommand {
        names: &["\\a"],
        args: "",
        help: "toggle between aligned and unaligned output",
        arg: ArgKind::None,
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
        arg: ArgKind::Setting,
        run: |sh, c| sh.pset(c.rest),
    },
    MetaCommand {
        names: &["\\timing"],
        args: "[on|off]",
        help: "toggle timing of statements",
        arg: ArgKind::Value(Setting::Timing),
        run: |sh, c| sh.set_option(Setting::Timing, c.arg()),
    },
    MetaCommand {
        names: &["\\o"],
        args: "[FILE]",
        help: "send results to FILE, or back to stdout",
        arg: ArgKind::Path,
        run: |sh, c| {
            match c.path() {
                Some(path) => sh.output_to_file(&path)?,
                None => sh.output_to_stdout(),
            }
            Ok(Flow::Continue)
        },
    },
    MetaCommand {
        names: &["\\i"],
        args: "FILE",
        help: "run the commands in FILE",
        arg: ArgKind::Path,
        run: |sh, c| match c.path() {
            Some(path) => sh.run_file(&path),
            None => sh.fail("\\i needs a file name"),
        },
    },
    MetaCommand {
        names: &["\\dump"],
        args: "DIR",
        help: "dump the database into DIR: its schema as SQL, its rows as Parquet",
        arg: ArgKind::Path,
        run: |sh, c| match c.path() {
            Some(dir) => sh.dump(&dir),
            None => sh.fail("\\dump needs a directory"),
        },
    },
    MetaCommand {
        names: &["\\restore"],
        args: "DIR",
        help: "restore the dump in DIR into this database",
        arg: ArgKind::Path,
        run: |sh, c| match c.path() {
            Some(dir) => sh.restore(&dir),
            None => sh.fail("\\restore needs a directory"),
        },
    },
    MetaCommand {
        names: &["\\backup"],
        args: "NAME",
        help: "copy the server's data directory, at a checkpoint, into its backup directory as NAME",
        arg: ArgKind::None,
        run: |sh, c| match c.arg() {
            Some(name) => sh.backup(name),
            None => sh.fail("\\backup needs a name"),
        },
    },
    MetaCommand {
        names: &["\\e"],
        args: "[FILE]",
        help: "edit the last statement, or FILE, in $EDITOR",
        arg: ArgKind::Path,
        run: |sh, c| sh.edit(c.path().as_deref()),
    },
    MetaCommand {
        names: &["\\echo"],
        args: "TEXT",
        help: "print TEXT",
        arg: ArgKind::None,
        run: |sh, c| {
            sh.write_result(&format!("{}\n", c.rest));
            Ok(Flow::Continue)
        },
    },
    MetaCommand {
        names: &["\\!"],
        args: "[COMMAND]",
        help: "run COMMAND in a shell",
        arg: ArgKind::None,
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

/// How a `\pset` value is spelled.
pub fn name_of(v: &impl clap::ValueEnum) -> String {
    v.to_possible_value()
        .expect("no variant is skipped")
        .get_name()
        .to_string()
}

/// Every variant's spelling.
pub fn variants<E: clap::ValueEnum>() -> Vec<String> {
    E::value_variants().iter().map(name_of).collect()
}

/// The variant `v` spells, in any case; the error lists [`variants`].
pub fn choose<E: clap::ValueEnum>(v: &str) -> Result<E, String> {
    E::from_str(v, true).map_err(|_| format!("\"{v}\" is not one of {}", variants::<E>().join(", ")))
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

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

/// The text of `v` when it lexes as one single-quoted string.
fn single_quoted(v: &str) -> Option<String> {
    match crate::lex::lex(v)?.as_slice() {
        [l] => match &l.token {
            Token::SingleQuotedString(text) => Some(text.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// `pattern`, a psql wildcard pattern, as a case-insensitive test of a
/// relation's or index's schema and name: of the qualified name when the
/// pattern has a `.`, else of the name. Double quotes are dropped, as psql reads
/// them as quoting; a catalog name holds none.
fn glob(pattern: Option<&str>) -> impl FnMut(&str, &str) -> bool {
    let pattern = pattern.map(|p| p.replace('"', ""));
    let qualified = pattern.as_ref().is_some_and(|p| p.contains('.'));
    let m = pattern.map(|p| LikeMatcher::compile(LikePattern::glob(&p).as_bytes(), true));
    let mut folded = Vec::new();
    move |schema, name| match &m {
        None => true,
        Some(m) if qualified => m.matches(qualified_key(schema, name).as_bytes(), &mut folded),
        Some(m) => m.matches(name.as_bytes(), &mut folded),
    }
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
        let call = Call {
            command: cmd,
            rest: line[name.end..].trim(),
            verbose,
        };
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
        let mut out = format!("General\n{KEY_HELP}\nMeta-commands\n");
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
        let key = RelName::schema_key(schema).ok();
        let Some(known) = key.filter(|k| catalog.schemas.contains(k)) else {
            return self.fail(format!("schema \"{schema}\" does not exist"));
        };
        self.schema = known;
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
        let mut matches = glob(pattern);
        let rows = catalog
            .relations
            .iter()
            .filter(|r| keep(r.class()) && matches(&r.schema, &r.name))
            .map(|r| {
                vec![
                    Value::Text(r.schema.clone()),
                    Value::Text(r.name.clone()),
                    Value::Text(r.class().describe().to_string()),
                    Value::Int(r.visible_columns().count() as i128),
                ]
            })
            .collect();
        let columns = [
            ("Schema", TypeCode::String),
            ("Name", TypeCode::String),
            ("Type", TypeCode::String),
            ("Columns", TypeCode::I64),
        ];
        self.print_listing("relations", &columns, rows)
    }

    fn list_indexes(&mut self, pattern: Option<&str>) -> Result<Flow, Failed> {
        let catalog = self.catalog()?;
        let mut matches = glob(pattern);
        let rows = catalog
            .relations
            .iter()
            .flat_map(|r| r.indexes.iter().map(move |i| (r, i)))
            .filter(|(r, i)| matches(&r.schema, &i.name))
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
        let columns = [
            ("Schema", TypeCode::String),
            ("Name", TypeCode::String),
            ("Table", TypeCode::String),
            ("Columns", TypeCode::String),
            ("Unique", TypeCode::Bool),
        ];
        self.print_listing("indexes", &columns, rows)
    }

    /// A listing's rows, or that it found none.
    fn print_listing(
        &mut self,
        noun: &str,
        columns: &[(&str, TypeCode)],
        rows: Vec<Vec<Value>>,
    ) -> Result<Flow, Failed> {
        match rows.is_empty() {
            true => self.status(&format!("Did not find any matching {noun}.")),
            false => self.print_rows(&Rows::of(columns, rows)),
        }
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
        let found = gnitz_sql::parse_relation_name(name, &self.schema).and_then(|n| catalog.get(&n));
        let Some(rel) = found else {
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
        if verbose && matches!(rel.props, RelProps::View(_)) {
            let definition = gnitz_sql::create_view_statement(catalog, rel).unwrap_or_else(|e| format!("({e})"));
            section("Definition", definition.lines().map(str::to_string).collect());
        }
        out
    }

    fn dump(&mut self, dir: &str) -> Result<Flow, Failed> {
        let done = gnitz_core::block_on(gnitz_dump::dump(&mut self.client, std::path::Path::new(dir)));
        let s = done.or_else(|e| self.fail(format!("\\dump: {e}")))?;
        self.status(&format!(
            "dumped {} schema(s), {} table(s), {} view(s) and {} row(s) into {dir}",
            s.schemas, s.tables, s.views, s.rows
        ));
        Ok(Flow::Continue)
    }

    fn restore(&mut self, dir: &str) -> Result<Flow, Failed> {
        let done = gnitz_core::block_on(gnitz_dump::restore(&mut self.client, std::path::Path::new(dir)));
        let s = done.or_else(|e| self.fail(format!("\\restore: {e}")))?;
        self.status(&format!(
            "restored {} schema(s), {} table(s), {} view(s) and {} row(s) from {dir}",
            s.schemas, s.tables, s.views, s.rows
        ));
        Ok(Flow::Continue)
    }

    fn backup(&mut self, name: &str) -> Result<Flow, Failed> {
        gnitz_core::block_on(self.client.backup(name)).or_else(|e| self.fail(format!("\\backup: {e}")))?;
        self.status(&format!("backed up as {name}"));
        Ok(Flow::Continue)
    }

    fn edit(&mut self, file: Option<&str>) -> Result<Flow, Failed> {
        if !self.interactive {
            return self.fail("\\e needs an interactive session");
        }
        let path = match file {
            Some(f) => PathBuf::from(f),
            None => {
                let path = self.scratch()?.join("edit.sql");
                std::fs::write(&path, &self.last_statement)
                    .or_else(|e| self.fail(format!("{}: {e}", path.display())))?;
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

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map_or_else(String::new, |f| f.to_uppercase().chain(c).collect())
}

#[cfg(test)]
#[path = "tests/meta.rs"]
mod tests;
