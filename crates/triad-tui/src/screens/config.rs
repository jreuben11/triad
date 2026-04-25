use crate::client::AppData;
use crate::widgets::key_help;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

pub fn render(
    frame: &mut Frame,
    area: Rect,
    _data: &AppData,
    scroll: u16,
    validate_result: &Option<Result<String, String>>,
) {
    let chunks = Layout::default()
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    render_config_tree(frame, chunks[0], scroll, validate_result);
    key_help::render_config(frame, chunks[1]);
}

fn render_config_tree(
    frame: &mut Frame,
    area: Rect,
    scroll: u16,
    validate_result: &Option<Result<String, String>>,
) {
    let mut lines = vec![
        Line::from(vec![Span::styled(
            "▼ backends",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from(vec![Span::raw("  ▼ postgres")]),
        Line::from(vec![Span::raw(
            "      url:          postgres://localhost/triad",
        )]),
        Line::from(vec![Span::raw("      pool_size:    16")]),
        Line::from(vec![Span::raw("      min_idle:     2")]),
        Line::from(vec![Span::raw("  ▼ kafka")]),
        Line::from(vec![Span::raw("      brokers:      [\"localhost:9092\"]")]),
        Line::from(vec![Span::raw("      group_id:     triad-runner")]),
        Line::from(vec![Span::raw("  ▼ redis")]),
        Line::from(vec![Span::raw("      mode:         standalone")]),
        Line::from(vec![Span::raw(
            "      url:          redis://localhost:6379",
        )]),
        Line::from(vec![Span::styled(
            "▼ patterns",
            Style::default().fg(Color::Yellow),
        )]),
        Line::raw("  (load triad.yaml to see patterns)"),
    ];

    if let Some(result) = validate_result {
        lines.push(Line::raw(""));
        match result {
            Ok(msg) => lines.push(Line::from(vec![Span::styled(
                format!("✓ {}", msg),
                Style::default().fg(Color::Green),
            )])),
            Err(msg) => lines.push(Line::from(vec![Span::styled(
                format!("✗ {}", msg),
                Style::default().fg(Color::Red),
            )])),
        }
    }

    let p = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("CONFIG  triad.yaml"),
        )
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(p, area);
}
