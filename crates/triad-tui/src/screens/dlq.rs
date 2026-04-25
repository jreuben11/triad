use crate::client::AppData;
use crate::widgets::key_help;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, data: &AppData, selected: usize, confirm_purge: bool) {
    let chunks = Layout::default()
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    render_list(frame, chunks[0], data, selected);
    key_help::render_dlq(frame, chunks[1]);

    if confirm_purge {
        render_confirm_popup(frame, area);
    }
}

fn render_list(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let header = ListItem::new(Line::from(vec![Span::styled(
        format!("  {:<40} {:>10}  {}", "Topic", "Messages", "Actions"),
        Style::default().fg(Color::DarkGray),
    )]));

    let mut items = vec![header];
    for (i, pattern) in data.patterns.iter().enumerate() {
        let topic = format!("triad.dlq.{}", pattern.name);
        let prefix = if i == selected { "▶" } else { " " };
        items.push(ListItem::new(Line::from(vec![
            Span::raw(format!("{} ", prefix)),
            Span::raw(format!("{:<40}", topic)),
            Span::raw(format!("  {:>10}  ", "?")),
            Span::styled("[R]eplay", Style::default().fg(Color::Cyan)),
            Span::raw("  "),
            Span::styled("[P]urge", Style::default().fg(Color::Red)),
        ])));
    }

    if data.patterns.is_empty() {
        items.push(ListItem::new(Line::from(vec![Span::styled(
            "  No DLQ topics",
            Style::default().fg(Color::DarkGray),
        )])));
    }

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title("DEAD LETTER QUEUES"),
    );
    frame.render_widget(list, area);
}

fn render_confirm_popup(frame: &mut Frame, area: Rect) {
    let popup_width = 40u16;
    let popup_height = 5u16;
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect::new(
        x,
        y,
        popup_width.min(area.width),
        popup_height.min(area.height),
    );

    frame.render_widget(Clear, popup_area);
    let popup = Paragraph::new(vec![
        Line::raw(""),
        Line::from(vec![Span::styled(
            "  Purge this DLQ? ",
            Style::default().add_modifier(Modifier::BOLD),
        )]),
        Line::raw(""),
        Line::from(vec![
            Span::styled("  [P]confirm  ", Style::default().fg(Color::Red)),
            Span::styled("[n]cancel", Style::default().fg(Color::Green)),
        ]),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Confirm Purge")
            .style(Style::default().fg(Color::Yellow)),
    );
    frame.render_widget(popup, popup_area);
}
