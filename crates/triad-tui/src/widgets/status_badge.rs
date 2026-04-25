use ratatui::style::Color;

pub fn badge_for(status: &str) -> (&'static str, Color) {
    match status {
        "Running" | "ok" => ("●", Color::Green),
        "Paused" => ("◌", Color::Yellow),
        "Error" | "error" => ("✗", Color::Red),
        "unconfigured" => ("○", Color::DarkGray),
        "Completed" => ("✓", Color::Cyan),
        _ => ("?", Color::Gray),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_badge_running() {
        let (sym, color) = badge_for("Running");
        assert_eq!(sym, "●");
        assert_eq!(color, Color::Green);
    }

    #[test]
    fn test_badge_ok() {
        let (sym, color) = badge_for("ok");
        assert_eq!(sym, "●");
        assert_eq!(color, Color::Green);
    }

    #[test]
    fn test_badge_paused() {
        let (sym, color) = badge_for("Paused");
        assert_eq!(sym, "◌");
        assert_eq!(color, Color::Yellow);
    }

    #[test]
    fn test_badge_error() {
        let (sym, color) = badge_for("Error");
        assert_eq!(sym, "✗");
        assert_eq!(color, Color::Red);
    }

    #[test]
    fn test_badge_completed() {
        let (sym, color) = badge_for("Completed");
        assert_eq!(sym, "✓");
        assert_eq!(color, Color::Cyan);
    }

    #[test]
    fn test_badge_unknown() {
        let (sym, color) = badge_for("Unknown");
        assert_eq!(sym, "?");
        assert_eq!(color, Color::Gray);
    }
}
