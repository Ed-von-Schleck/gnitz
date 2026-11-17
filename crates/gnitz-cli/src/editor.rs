//! The interactive session: reedline, with the highlighter, completer,
//! validator and prompt of this shell, reading what the shell runs.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use gnitz_core::block_on;
use nu_ansi_term::{Color, Style};
use reedline::{
    DefaultHinter, DescriptionMode, EditCommand, Emacs, FileBackedHistory, IdeMenu, InputMode, KeyCode, KeyModifiers,
    Keybindings, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus, PromptViMode,
    Reedline, ReedlineEvent, ReedlineMenu, Signal, ValidationResult, Validator, Vi, default_emacs_keybindings,
    default_vi_insert_keybindings, default_vi_normal_keybindings, default_vi_visual_keybindings,
};

use crate::complete::Context;
use crate::highlight::SqlHighlighter;
use crate::lex;
use crate::shell::{Flow, Shell, command_from_env};

const COMPLETION_MENU: &str = "completion_menu";
const HISTORY_CAPACITY: usize = 10_000;

/// A buffer is submitted once it ends a statement or is a meta-command.
struct SqlValidator;

impl Validator for SqlValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        match lex::is_complete(line) {
            true => ValidationResult::Complete,
            false => ValidationResult::Incomplete,
        }
    }
}

/// `schema=>`, psql's shape: `=*>` inside a transaction, `->` on a
/// continuation line.
struct ShellPrompt {
    schema: String,
    in_transaction: bool,
}

impl Prompt for ShellPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.schema)
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, mode: PromptEditMode) -> Cow<'_, str> {
        let mark = match mode {
            PromptEditMode::Vi(PromptViMode::Normal) => ":",
            _ => "=",
        };
        let txn = if self.in_transaction { "*" } else { "" };
        Cow::Owned(format!("{mark}{txn}> "))
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Owned(format!("{}-> ", " ".repeat(self.schema.chars().count())))
    }

    fn render_prompt_history_search_indicator(&self, search: PromptHistorySearch) -> Cow<'_, str> {
        let prefix = match search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!("({prefix}reverse-search: {}) ", search.term))
    }

    fn get_prompt_color(&self) -> Color {
        Color::Green
    }

    fn get_indicator_color(&self) -> Color {
        match self.in_transaction {
            true => Color::Yellow,
            false => Color::Green,
        }
    }
}

/// `~/.local/share/gnitz/history` (or the platform's data directory).
fn history_path() -> Option<PathBuf> {
    let dir = dirs::data_dir()?.join("gnitz");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir.join("history"))
}

/// What `\?` says of the keys [`add_common_bindings`] binds.
pub const KEY_HELP: &str = "  SQL statements end with ';'. Tab completes, Ctrl-R searches history,\n  Alt-Enter inserts a newline, Alt-E opens the buffer in $EDITOR.\n";

fn add_common_bindings(kb: &mut Keybindings) {
    kb.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu(COMPLETION_MENU.to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );
    kb.add_binding(KeyModifiers::SHIFT, KeyCode::BackTab, ReedlineEvent::MenuPrevious);
    kb.add_binding(
        KeyModifiers::ALT,
        KeyCode::Enter,
        ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
    );
    kb.add_binding(KeyModifiers::ALT, KeyCode::Char('e'), ReedlineEvent::OpenEditor);
}

/// `$VISUAL`, else `$EDITOR`, else `vi`.
pub fn editor() -> String {
    command_from_env(&["VISUAL", "EDITOR"], "vi")
}

/// The user's editor, run on `path` through the shell so `$EDITOR` may carry
/// arguments.
pub fn editor_command(path: &Path) -> Command {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(format!("{} \"$1\"", editor())).arg("sh").arg(path);
    cmd
}

/// The editor, without a completer: [`run`] sets one at each prompt. Alt-E
/// writes the buffer to `buffer` for `$EDITOR`.
fn build(vi: bool, color: bool, buffer: PathBuf) -> Reedline {
    // The whole buffer, so the completer reads references past the cursor.
    let menu = IdeMenu::default()
        .with_name(COMPLETION_MENU)
        .with_input_mode(InputMode::FullBuffer)
        .with_default_border()
        .with_description_mode(DescriptionMode::PreferRight)
        .with_max_completion_height(12)
        .with_max_description_width(40);
    let edit_mode: Box<dyn reedline::EditMode> = match vi {
        true => {
            let mut insert = default_vi_insert_keybindings();
            add_common_bindings(&mut insert);
            Box::new(Vi::new(
                insert,
                default_vi_normal_keybindings(),
                default_vi_visual_keybindings(),
            ))
        }
        false => {
            let mut kb = default_emacs_keybindings();
            add_common_bindings(&mut kb);
            Box::new(Emacs::new(kb))
        }
    };
    let editor = editor_command(&buffer);
    let mut line_editor = Reedline::create()
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
        .with_validator(Box::new(SqlValidator))
        .with_edit_mode(edit_mode)
        .with_quick_completions(false)
        .with_partial_completions(true)
        .with_buffer_editor(editor, buffer)
        .with_ansi_colors(color);
    if color {
        line_editor = line_editor
            .with_highlighter(Box::new(SqlHighlighter::default()))
            .with_hinter(Box::new(
                DefaultHinter::default().with_style(Style::new().fg(Color::DarkGray)),
            ));
    }
    if let Some(path) = history_path()
        && let Ok(history) = FileBackedHistory::with_file(HISTORY_CAPACITY, path)
    {
        line_editor = line_editor.with_history(Box::new(history));
    }
    line_editor
}

/// The session: read a buffer at each prompt and run it, until `\q`, an exit
/// word or Ctrl-D.
pub fn run(shell: &mut Shell, vi: bool) -> ExitCode {
    // Alt-E's buffer file lives here, and `\e`'s.
    let scratch = match gnitz_foundation::posix_io::ScratchDir::new("gnitz-") {
        Ok(dir) => shell.scratch.insert(dir),
        Err(e) => {
            eprintln!("gnitz: cannot make a scratch directory: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut line_editor = build(vi, shell.options.color, scratch.path().join("buffer.sql"));
    println!(
        "gnitz {} — connected to {}. Type \\? for help, \\q to quit.",
        gnitz_wire::RELEASE,
        shell.options.target
    );
    loop {
        // Completion offers what the catalog holds now, whichever session
        // changed it; a failed read keeps the previous completer. A Ctrl-C
        // left over from the last command must not cancel the read.
        shell.clear_interrupt();
        if let Ok(catalog) = block_on(shell.client.catalog()) {
            let schema = shell.options.schema.clone();
            line_editor = line_editor.with_completer(Box::new(Context { catalog, schema }));
        }
        if let Some(text) = shell.prefill.take() {
            line_editor.run_edit_commands(&[EditCommand::InsertString(text)]);
        }
        let prompt = ShellPrompt {
            schema: shell.options.schema.clone(),
            in_transaction: shell.client.txn_active(),
        };
        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(buffer)) => {
                // On disk before it runs, so a session a signal ends keeps it.
                let _ = line_editor.sync_history();
                if let Ok(Flow::Quit) = shell.run_script(&buffer) {
                    break;
                }
            }
            Ok(Signal::CtrlD) => break,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("gnitz: terminal error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
