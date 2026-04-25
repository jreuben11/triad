use crate::client::AppData;
use crate::widgets::{key_help, lag_bar, status_badge};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, data: &AppData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(area);

    render_header(frame, chunks[0], data);

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    render_patterns_panel(frame, main[0], data);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(main[1]);

    render_backends_panel(frame, right[0], data);
    render_lag_panel(frame, right[1], data);

    key_help::render_dashboard(frame, chunks[2]);
}

fn render_header(frame: &mut Frame, area: Rect, data: &AppData) {
    // Derive status from ready endpoint (reflects backend health) when available;
    // fall back to live endpoint (process-alive only) when ready data is not yet fetched.
    let (status, status_color) = if data.connecting {
        ("CONNECTING…".to_string(), Color::Yellow)
    } else if let Some(ready) = &data.ready {
        match ready.status.as_str() {
            "ok" => ("RUNNING".to_string(), Color::Green),
            "degraded" => ("DEGRADED".to_string(), Color::Red),
            _ => ("UNKNOWN".to_string(), Color::Red),
        }
    } else if data.live.is_some() {
        ("RUNNING".to_string(), Color::Green)
    } else {
        ("UNKNOWN".to_string(), Color::Red)
    };

    let uptime = data
        .live
        .as_ref()
        .map(|l| format_uptime(l.uptime_seconds))
        .unwrap_or_default();
    let leader = data
        .ready
        .as_ref()
        .map(|r| if r.leader { "✓" } else { "✗" })
        .unwrap_or("?");

    let line = Line::from(vec![
        Span::styled("▸ triad  ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(format!("■ {}  ", status), Style::default().fg(status_color)),
        Span::raw(format!("↑ {}  leader: {}  v0.1.0", uptime, leader)),
    ]);
    let header = Paragraph::new(line).block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, area);
}

fn render_patterns_panel(frame: &mut Frame, area: Rect, data: &AppData) {
    let running = data
        .patterns
        .iter()
        .filter(|p| p.status == "Running")
        .count();
    let total = data.patterns.len();
    let title = format!("PATTERNS  {}/{} running", running, total);

    let items: Vec<ListItem> = data
        .patterns
        .iter()
        .map(|p| {
            let (symbol, color) = status_badge::badge_for(&p.status);
            ListItem::new(Line::from(vec![
                Span::styled(format!(" {} ", symbol), Style::default().fg(color)),
                Span::raw(format!("{:<20}", p.name)),
                Span::styled(p.status.clone(), Style::default().fg(color)),
            ]))
        })
        .collect();

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(list, area);
}

fn render_backends_panel(frame: &mut Frame, area: Rect, data: &AppData) {
    let mut lines = vec![];
    if let Some(ready) = &data.ready {
        for (name, backend) in &ready.backends {
            let color = if backend.status == "ok" {
                Color::Green
            } else if backend.status == "unconfigured" {
                Color::DarkGray
            } else {
                Color::Red
            };
            let (sym, _) = status_badge::badge_for(&backend.status);
            lines.push(Line::from(vec![
                Span::styled(format!(" {} ", sym), Style::default().fg(color)),
                Span::raw(format!("{:<12}", name)),
                Span::styled(backend.status.clone(), Style::default().fg(color)),
            ]));
        }
    } else {
        lines.push(Line::raw("  No backend data"));
    }

    let p = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("BACKENDS"));
    frame.render_widget(p, area);
}

fn render_lag_panel(frame: &mut Frame, area: Rect, data: &AppData) {
    lag_bar::render(frame, area, &data.lag);
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    format!("{}h {}m", h, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uptime_zero() {
        assert_eq!(format_uptime(0), "0h 0m");
    }

    #[test]
    fn test_format_uptime_one_hour() {
        assert_eq!(format_uptime(3600), "1h 0m");
    }

    #[test]
    fn test_format_uptime_mixed() {
        assert_eq!(format_uptime(7320), "2h 2m");
    }
}
