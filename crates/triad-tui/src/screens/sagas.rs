use crate::client::AppData;
use crate::widgets::key_help;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, data: &AppData, selected: usize, expanded: bool) {
    let chunks = if expanded && !data.sagas.is_empty() {
        Layout::default()
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area)
    } else {
        Layout::default()
            .constraints([Constraint::Min(0), Constraint::Length(3)])
            .split(area)
    };

    render_list(frame, chunks[0], data, selected);

    if expanded && !data.sagas.is_empty() {
        render_detail(frame, chunks[1], data, selected);
    } else {
        key_help::render_sagas(frame, chunks[1]);
    }
}

fn render_list(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let header = ListItem::new(Line::from(vec![Span::styled(
        format!(
            "  {:<28} {:<12} {:<14} {}",
            "Saga ID", "Name", "Status", "Step"
        ),
        Style::default().fg(Color::DarkGray),
    )]));

    let mut items = vec![header];
    for (i, saga) in data.sagas.iter().enumerate() {
        let prefix = if i == selected { "▶" } else { " " };
        let id_short = if saga.saga_id.len() > 10 {
            format!(
                "{}…{}",
                &saga.saga_id[..4],
                &saga.saga_id[saga.saga_id.len() - 4..]
            )
        } else {
            saga.saga_id.clone()
        };
        let status_color = match saga.status.as_str() {
            "Running" => Color::Green,
            "Completed" => Color::Cyan,
            "RolledBack" | "Cancelled" => Color::Red,
            _ => Color::Yellow,
        };
        items.push(ListItem::new(Line::from(vec![
            Span::raw(format!("{} ", prefix)),
            Span::raw(format!("{:<28} ", id_short)),
            Span::raw(format!("{:<12} ", saga.saga_name)),
            Span::styled(
                format!("{:<14} ", saga.status),
                Style::default().fg(status_color),
            ),
            Span::raw(saga.current_step.to_string()),
        ])));
    }

    if data.sagas.is_empty() {
        items.push(ListItem::new(Line::from(vec![Span::styled(
            "  No saga data",
            Style::default().fg(Color::DarkGray),
        )])));
    }

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("SAGAS"));
    frame.render_widget(list, area);
}

fn render_detail(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let Some(saga) = data.sagas.get(selected) else {
        return;
    };

    let lines = vec![
        Line::from(vec![Span::styled(
            format!("▼ SAGA DETAIL: {} — {}", saga.saga_id, saga.saga_name),
            Style::default().fg(Color::Yellow),
        )]),
        Line::raw(""),
        Line::from(vec![Span::raw(format!(
            "  Status: {} | Step: {}",
            saga.status, saga.current_step
        ))]),
        Line::raw(""),
        Line::from(vec![Span::styled(
            "  [c]ancel saga",
            Style::default().fg(Color::Red),
        )]),
    ];

    let p = Paragraph::new(lines).block(Block::default().borders(Borders::ALL));
    frame.render_widget(p, area);
}
