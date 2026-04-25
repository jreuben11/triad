use crate::client::AppData;
use crate::widgets::key_help;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem},
};

pub fn render(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let chunks = Layout::default()
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    render_table(frame, chunks[0], data, selected);
    key_help::render_checkpoints(frame, chunks[1]);
}

fn render_table(frame: &mut Frame, area: Rect, data: &AppData, selected: usize) {
    let header = ListItem::new(Line::from(vec![Span::styled(
        format!(
            "  {:<22} {:<18} {:>10}  {}",
            "Pattern", "Pipeline", "Offset", "Updated"
        ),
        Style::default().fg(Color::DarkGray),
    )]));

    let mut items = vec![header];
    for (i, cp) in data.checkpoints.iter().enumerate() {
        let prefix = if i == selected { "▶" } else { " " };
        items.push(ListItem::new(Line::from(vec![
            Span::raw(format!("{} ", prefix)),
            Span::raw(format!("{:<22} ", cp.pattern_name)),
            Span::raw(format!("{:<18} ", cp.pipeline_name)),
            Span::raw(format!("{:>10}  ", cp.version)),
            Span::styled("just now", Style::default().fg(Color::DarkGray)),
        ])));
    }

    if data.checkpoints.is_empty() {
        items.push(ListItem::new(Line::from(vec![Span::styled(
            "  No checkpoint data",
            Style::default().fg(Color::DarkGray),
        )])));
    }

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("CHECKPOINTS"));
    frame.render_widget(list, area);
}
