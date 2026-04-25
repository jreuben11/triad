use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

fn render_help(frame: &mut Frame, area: Rect, keys: &[(&str, &str)]) {
    let mut spans: Vec<Span> = vec![Span::raw("  ")];
    for (i, (key, desc)) in keys.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            format!("[{}]", key),
            Style::default().fg(Color::Cyan),
        ));
        spans.push(Span::raw(desc.to_string()));
    }
    let p = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::ALL));
    frame.render_widget(p, area);
}

pub fn render_dashboard(frame: &mut Frame, area: Rect) {
    render_help(
        frame,
        area,
        &[
            ("1", "Dashboard"),
            ("2", "Patterns"),
            ("3", "DLQ"),
            ("4", "Checkpoints"),
            ("5", "Sagas"),
            ("6", "Config"),
            ("r", "efresh"),
            ("q", "uit"),
        ],
    );
}

pub fn render_patterns(frame: &mut Frame, area: Rect) {
    render_help(
        frame,
        area,
        &[
            ("↑/↓", "select"),
            ("p", "pause"),
            ("r", "resume"),
            ("x", "replay"),
            ("ESC", "dashboard"),
        ],
    );
}

pub fn render_dlq(frame: &mut Frame, area: Rect) {
    render_help(
        frame,
        area,
        &[
            ("↑/↓", "select"),
            ("R", "replay"),
            ("P", "purge"),
            ("ESC", "dashboard"),
        ],
    );
}

pub fn render_checkpoints(frame: &mut Frame, area: Rect) {
    render_help(frame, area, &[("↑/↓", "select"), ("ESC", "dashboard")]);
}

pub fn render_sagas(frame: &mut Frame, area: Rect) {
    render_help(
        frame,
        area,
        &[
            ("↑/↓", "select"),
            ("Enter", "expand"),
            ("c", "cancel"),
            ("ESC", "dashboard"),
        ],
    );
}

pub fn render_config(frame: &mut Frame, area: Rect) {
    render_help(
        frame,
        area,
        &[("↑/↓", "scroll"), ("v", "validate"), ("ESC", "dashboard")],
    );
}
