//! The line editor: reedline, with the highlighter, completer, validator and
//! prompt of this shell.

use std::borrow::Cow;
use std::path::PathBuf;

use nu_ansi_term::{Color, Style};
use reedline::{
    default_emacs_keybindings, default_vi_insert_keybindings, default_vi_normal_keybindings,
    default_vi_visual_keybindings, DefaultHinter, DescriptionMode, EditCommand, Emacs, FileBackedHistory, IdeMenu,
    KeyCode, KeyModifiers, Keybindings, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch,
    PromptHistorySearchStatus, PromptViMode, Reedline, ReedlineEvent, ReedlineMenu, ValidationResult, Validator, Vi,
};

use crate::complete::{SharedContext, SqlCompleter};
use crate::highlight::SqlHighlighter;
use crate::lex;
use crate::meta::editor_command;

const COMPLETION_MENU: &str = "completion_menu";
const HISTORY_CAPACITY: usize = 10_000;

/// A buffer is submitted once it ends a statement or is a meta-command.
pub struct SqlValidator;

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
pub struct ShellPrompt {
    pub schema: String,
    pub in_transaction: bool,
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

    fn get_prompt_color(&self) -> reedline::Color {
        reedline::Color::Green
    }

    fn get_indicator_color(&self) -> reedline::Color {
        match self.in_transaction {
            true => reedline::Color::Yellow,
            false => reedline::Color::Green,
        }
    }
}

/// `~/.local/share/gnitz/history` (or the platform's data directory).
fn history_path() -> Option<PathBuf> {
    let dir = dirs::data_dir()?.join("gnitz");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir.join("history"))
}

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

/// The editor, reading completions from `context`.
pub fn build(context: SharedContext, vi: bool, color: bool) -> Reedline {
    let menu = IdeMenu::default()
        .with_name(COMPLETION_MENU)
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
    let temp = std::env::temp_dir().join(format!("gnitz-buffer-{}.sql", std::process::id()));
    let editor = editor_command(&temp);
    let mut line_editor = Reedline::create()
        .with_completer(Box::new(SqlCompleter::new(context)))
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
        .with_validator(Box::new(SqlValidator))
        .with_edit_mode(edit_mode)
        .with_quick_completions(false)
        .with_partial_completions(true)
        .with_buffer_editor(editor, temp)
        .with_ansi_colors(color);
    if color {
        line_editor = line_editor
            .with_highlighter(Box::new(SqlHighlighter::default()))
            .with_hinter(Box::new(
                DefaultHinter::default().with_style(Style::new().fg(Color::DarkGray)),
            ));
    }
    if let Some(path) = history_path() {
        if let Ok(history) = FileBackedHistory::with_file(HISTORY_CAPACITY, path) {
            line_editor = line_editor.with_history(Box::new(history));
        }
    }
    line_editor
}
