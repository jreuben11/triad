use crate::client::AppData;
use crate::widgets::{key_help, status_badge};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
};

pub fn render(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let chunks = Layout::default()
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    render_list(frame, chunks[0], data, selected);
    key_help::render_patterns(frame, chunks[1]);
}

fn render_list(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let header = ListItem::new(Line::from(vec![Span::styled(
        format!(
            "  {:<20} {:<15} {:<12} {}",
            "Name", "Type", "Status", "Actions"
        ),
        Style::default().fg(Color::DarkGray),
    )]));

    let mut items = vec![header];
    for (i, p) in data.patterns.iter().enumerate() {
        let (sym, color) = status_badge::badge_for(&p.status);
        let prefix = if i == selected { "▶" } else { " " };
        let actions = match p.status.as_str() {
            "Running" => "[p]ause  [x]replay",
            "Paused" => "[r]esume [x]replay",
            _ => "[r]esume",
        };
        items.push(ListItem::new(Line::from(vec![
            Span::raw(format!("{} ", prefix)),
            Span::raw(format!("{:<20} ", p.name)),
            Span::raw(format!("{:<15} ", p.pattern_type)),
            Span::styled(
                format!("{} {:<10}", sym, p.status),
                Style::default().fg(color),
            ),
            Span::raw(format!("  {}", actions)),
        ])));
    }

    if data.patterns.is_empty() {
        items.push(ListItem::new(Line::from(vec![Span::styled(
            "  No pattern data",
            Style::default().fg(Color::DarkGray),
        )])));
    }

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("PATTERNS"));
    let mut state = ListState::default();
    state.select(Some(selected + 1)); // +1 for header
    frame.render_stateful_widget(list, area, &mut state);
}
