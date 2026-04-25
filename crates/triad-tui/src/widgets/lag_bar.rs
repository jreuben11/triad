use crate::client::LagInfo;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

pub fn render(frame: &mut Frame, area: Rect, lag: &[LagInfo]) {
    let max_lag = lag.iter().map(|l| l.lag_messages).max().unwrap_or(1).max(1);
    let bar_width = (area.width as i32 - 30).max(6) as u64;

    let lines: Vec<Line> = lag
        .iter()
        .take((area.height as usize).saturating_sub(2))
        .map(|entry| {
            let filled = ((entry.lag_messages as u64 * bar_width) / max_lag as u64) as usize;
            let empty = (bar_width as usize).saturating_sub(filled);
            let bar_color = if entry.lag_messages > 1000 {
                Color::Red
            } else if entry.lag_messages > 100 {
                Color::Yellow
            } else {
                Color::Green
            };

            Line::from(vec![
                Span::raw(format!("  {:<20} ", truncate(&entry.topic, 20))),
                Span::styled("█".repeat(filled), Style::default().fg(bar_color)),
                Span::raw("░".repeat(empty)),
                Span::raw(format!("  {}", entry.lag_messages)),
            ])
        })
        .collect();

    let lines = if lines.is_empty() {
        vec![Line::styled(
            "  No lag data",
            Style::default().fg(Color::DarkGray),
        )]
    } else {
        lines
    };

    let p =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("CONSUMER LAG"));
    frame.render_widget(p, area);
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long_string() {
        let result = truncate("very-long-topic-name", 10);
        assert!(result.len() <= 12); // truncated + ellipsis
        assert!(result.ends_with('…'));
    }

    #[test]
    fn test_truncate_exact_length() {
        assert_eq!(truncate("hello", 5), "hello");
    }
}
