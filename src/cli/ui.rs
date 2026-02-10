use super::app::App;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Clear, List, ListItem, ListState},
};

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Min(1),     // Output area grows
                Constraint::Length(10), // Input area increased for multi-line comfort
            ]
            .as_ref(),
        )
        .split(f.area());

    // Output Area
    // Logic to show only the last N messages that fit
    let inner_height = chunks[0].height.saturating_sub(2) as usize; // -2 for borders
    let skip_count = app.messages.len().saturating_sub(inner_height);

    let messages: Vec<ListItem> = app
        .messages
        .iter()
        .skip(skip_count)
        .map(|line| {
            // Since app.messages stores Line<'static>, we can clone it relatively cheaply
            // or better yet, ListItem accepts Line directly.
            ListItem::new(line.clone())
        })
        .collect();

    let messages_panel = List::new(messages)
        .block(Block::default().borders(Borders::ALL).title(" Output "))
        .style(Style::default().fg(Color::White));

    f.render_widget(messages_panel, chunks[0]);

    // Input Area
    let textarea_rect = chunks[1];
    f.render_widget(&app.textarea, textarea_rect);

    // Autocomplete Popup
    if app.popup_open && !app.suggestions.is_empty() {
        let cursor = app.textarea.cursor();
        let (row, col) = (cursor.0, cursor.1);

        // Calculate absolute position
        // Input starts at chunks[1].y + 1 (border)
        // We want popup below the current line.
        let popup_x = textarea_rect.x + (col as u16) + 1;
        let popup_y = textarea_rect.y + (row as u16) + 2;

        // Clamp to screen bounds
        let width = 30;
        let height = 5.min(app.suggestions.len() as u16 + 2); // +2 for borders

        let area = Rect::new(
            popup_x.min(f.area().width - width),
            popup_y.min(f.area().height - height),
            width,
            height,
        );

        f.render_widget(Clear, area); // Clear underlying text

        let items: Vec<ListItem> = app
            .suggestions
            .iter()
            .map(|s| ListItem::new(s.as_str()))
            .collect();

        let mut state = ListState::default();
        state.select(Some(app.suggestion_index));

        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Suggestions"))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .highlight_symbol(">> ");

        f.render_stateful_widget(list, area, &mut state);
    }
}
