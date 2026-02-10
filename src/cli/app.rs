use super::autocomplete::Autocompleter;
use super::ui;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::{Backend, CrosstermBackend},
    style::{Color, Modifier, Style},
    text::{Line, Span},
};
use rustmemodb::facade::InMemoryDB;
use std::{error::Error, io};
use tui_textarea::{Input, Key, TextArea};

pub struct App<'a> {
    pub textarea: TextArea<'a>,
    pub messages: Vec<Line<'static>>,
    pub db: InMemoryDB,
    pub exit: bool,

    // Autocomplete State
    pub autocompleter: Autocompleter,
    pub suggestions: Vec<String>,
    pub suggestion_index: usize,
    pub popup_open: bool,
}

impl<'a> App<'a> {
    pub fn new() -> Self {
        let textarea = Self::new_textarea();

        Self {
            textarea,
            messages: vec![
                Line::from(vec![Span::styled(
                    "Welcome to RustMemDB CLI!",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )]),
                Line::from("Type your SQL query below and press Ctrl+E to execute."),
                Line::from("--------------------------------------------------"),
            ],
            db: InMemoryDB::new(),
            exit: false,
            autocompleter: Autocompleter::new(),
            suggestions: Vec::new(),
            suggestion_index: 0,
            popup_open: false,
        }
    }

    fn new_textarea() -> TextArea<'static> {
        let mut textarea = TextArea::default();
        textarea.set_placeholder_text(
            "Enter SQL query (Press 'Ctrl+E' to execute, 'Tab' for autocomplete, 'Esc' to quit)",
        );
        textarea.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title(" SQL Input "),
        );
        textarea
    }

    fn reset_textarea(&mut self) {
        self.textarea = Self::new_textarea();
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let res = self.run_loop(&mut terminal).await;

        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        if let Err(err) = res {
            println!("{:?}", err);
        }

        Ok(())
    }

    async fn run_loop<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> io::Result<()> {
        loop {
            terminal.draw(|f| ui::draw(f, self))?;

            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    // Navigation inside Popup
                    if self.popup_open {
                        match key.code {
                            KeyCode::Down => {
                                if !self.suggestions.is_empty() {
                                    self.suggestion_index =
                                        (self.suggestion_index + 1) % self.suggestions.len();
                                }
                                continue;
                            }
                            KeyCode::Up => {
                                if !self.suggestions.is_empty() {
                                    self.suggestion_index =
                                        (self.suggestion_index + self.suggestions.len() - 1)
                                            % self.suggestions.len();
                                }
                                continue;
                            }
                            KeyCode::Enter | KeyCode::Tab => {
                                self.accept_suggestion();
                                continue;
                            }
                            KeyCode::Esc => {
                                self.popup_open = false;
                                continue;
                            }
                            _ => {
                                // Close popup if user types something else, but let it pass to textarea
                                self.popup_open = false;
                            }
                        }
                    }

                    // Standard Editor Control
                    match key.code {
                        KeyCode::Esc => {
                            self.exit = true;
                            return Ok(());
                        }
                        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            self.execute_query().await;
                        }
                        KeyCode::Tab => {
                            // Trigger autocomplete manually
                            self.update_suggestions().await;
                            if !self.suggestions.is_empty() {
                                self.popup_open = true;
                                self.suggestion_index = 0;
                            }
                        }
                        _ => {
                            self.textarea.input(key);
                            // Auto-trigger on typing letters
                            if let KeyCode::Char(c) = key.code {
                                if c.is_alphabetic() || c == '_' {
                                    self.update_suggestions().await;
                                    if !self.suggestions.is_empty() {
                                        self.popup_open = true;
                                    } else {
                                        self.popup_open = false;
                                    }
                                } else {
                                    self.popup_open = false;
                                }
                            }
                        }
                    }
                }
            }
            if self.exit {
                return Ok(());
            }
        }
    }

    async fn update_suggestions(&mut self) {
        let (word, _) = self.get_current_word();
        if word.len() < 1 {
            self.suggestions.clear();
            self.popup_open = false;
            return;
        }

        self.suggestions = self.autocompleter.get_suggestions(&word, &self.db).await;
        self.suggestion_index = 0;
    }

    fn accept_suggestion(&mut self) {
        if self.suggestions.is_empty() {
            return;
        }
        let suggestion = self.suggestions[self.suggestion_index].clone();
        let (word, _start_col) = self.get_current_word();

        let cursor = self.textarea.cursor();
        let _line = cursor.0;
        let _col = cursor.1;

        // Delete characters
        for _ in 0..word.chars().count() {
            self.textarea.input(Input {
                key: Key::Backspace,
                ctrl: false,
                alt: false,
                shift: false,
            });
        }

        // Insert complete suggestion
        self.textarea.insert_str(&suggestion);
        self.popup_open = false;
    }

    fn get_current_word(&self) -> (String, usize) {
        let cursor = self.textarea.cursor();
        let line_idx = cursor.0;
        let col_idx = cursor.1;

        if line_idx >= self.textarea.lines().len() {
            return (String::new(), 0);
        }

        let line = &self.textarea.lines()[line_idx];
        if col_idx == 0 {
            return (String::new(), 0);
        }

        // Safe slicing using chars
        let prefix_chars: Vec<char> = line.chars().take(col_idx).collect();
        let prefix_str: String = prefix_chars.iter().collect();

        // Search backwards
        if let Some(start_char_idx) = prefix_chars
            .iter()
            .rposition(|&c| !c.is_alphanumeric() && c != '_')
        {
            // start_char_idx is the index of the separator
            let word: String = prefix_chars[start_char_idx + 1..].iter().collect();
            (word, start_char_idx + 1)
        } else {
            (prefix_str, 0)
        }
    }

    async fn execute_query(&mut self) {
        let input = self.textarea.lines().join("\n");
        if input.trim().is_empty() {
            return;
        }

        // 1. Log the query
        self.messages.push(Line::from(vec![
            Span::styled("> ", Style::default().fg(Color::Yellow)),
            Span::raw(input.clone()),
        ]));

        // 2. Clear input
        self.reset_textarea();

        // 3. Execute and Log Result
        match self.db.execute(&input).await {
            Ok(result) => {
                if result.rows().is_empty() {
                    self.messages.push(Line::from(Span::styled(
                        format!("OK. Rows affected: {}", result.row_count()),
                        Style::default().fg(Color::Green),
                    )));
                } else {
                    self.messages.push(Line::from(Span::styled(
                        format!("Success. {} rows found:", result.row_count()),
                        Style::default().fg(Color::Green),
                    )));

                    // Header
                    let header: Vec<String> =
                        result.columns().iter().map(|c| c.name.clone()).collect();
                    self.messages.push(Line::from(Span::styled(
                        format!("| {:?} |", header),
                        Style::default().add_modifier(Modifier::BOLD),
                    )));

                    // Rows
                    for row in result.rows() {
                        let row_str: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                        self.messages.push(Line::from(format!("| {:?} |", row_str)));
                    }
                }
            }
            Err(e) => {
                self.messages.push(Line::from(Span::styled(
                    format!("Error: {}", e),
                    Style::default().fg(Color::Red),
                )));
            }
        }
        self.messages.push(Line::from(Span::styled(
            "-".repeat(50),
            Style::default().fg(Color::DarkGray),
        )));
    }
}
