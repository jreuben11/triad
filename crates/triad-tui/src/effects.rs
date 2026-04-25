use ratatui::style::Color;
use tachyonfx::fx::{Direction, Glitch};
use tachyonfx::{Duration, Effect, EffectTimer, Interpolation, IntoEffect, fx};

pub fn startup_glitch() -> Effect {
    fx::with_duration(
        Duration::from_millis(600),
        Glitch::builder()
            .cell_glitch_ratio(0.2)
            .action_start_delay_ms(0..30)
            .action_ms(30..150)
            .build()
            .into_effect(),
    )
}

pub fn slide_forward() -> Effect {
    fx::slide_in(
        Direction::LeftToRight,
        10,
        0,
        Color::Black,
        EffectTimer::from_ms(250, Interpolation::CubicInOut),
    )
}

pub fn slide_back() -> Effect {
    fx::slide_in(
        Direction::RightToLeft,
        10,
        0,
        Color::Black,
        EffectTimer::from_ms(250, Interpolation::CubicInOut),
    )
}

#[allow(dead_code)]
pub fn pattern_status_fade(color: Color) -> Effect {
    fx::fade_from_fg(color, EffectTimer::from_ms(400, Interpolation::Linear))
}

#[allow(dead_code)]
pub fn confirm_popup_coalesce() -> Effect {
    fx::coalesce(EffectTimer::from_ms(300, Interpolation::CubicInOut))
}

#[allow(dead_code)]
pub fn config_validate_ok() -> Effect {
    fx::fade_from_fg(
        Color::Green,
        EffectTimer::from_ms(500, Interpolation::SineIn),
    )
}

#[allow(dead_code)]
pub fn config_validate_error() -> Effect {
    fx::with_duration(
        Duration::from_millis(400),
        Glitch::builder()
            .cell_glitch_ratio(0.3)
            .action_start_delay_ms(0..10)
            .action_ms(20..100)
            .build()
            .into_effect(),
    )
}

#[allow(dead_code)]
pub fn dlq_count_alert() -> Effect {
    fx::fade_from_fg(Color::Red, EffectTimer::from_ms(300, Interpolation::Linear))
}

#[allow(dead_code)]
pub fn action_success() -> Effect {
    fx::fade_from_fg(
        Color::Cyan,
        EffectTimer::from_ms(250, Interpolation::Linear),
    )
}

#[allow(dead_code)]
pub fn data_refresh_pulse() -> Effect {
    fx::fade_from_fg(
        Color::DarkGray,
        EffectTimer::from_ms(150, Interpolation::Linear),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyonfx::Shader;

    #[test]
    fn test_startup_glitch_created() {
        let effect = startup_glitch();
        assert!(effect.running());
    }

    #[test]
    fn test_slide_forward_created() {
        let effect = slide_forward();
        assert!(effect.running());
    }

    #[test]
    fn test_slide_back_created() {
        let effect = slide_back();
        assert!(effect.running());
    }

    #[test]
    fn test_pattern_status_fade_created() {
        let effect = pattern_status_fade(Color::Green);
        assert!(effect.running());
    }

    #[test]
    fn test_confirm_popup_coalesce_created() {
        let effect = confirm_popup_coalesce();
        assert!(effect.running());
    }

    #[test]
    fn test_config_validate_ok_created() {
        let effect = config_validate_ok();
        assert!(effect.running());
    }

    #[test]
    fn test_config_validate_error_created() {
        let effect = config_validate_error();
        assert!(effect.running());
    }

    #[test]
    fn test_dlq_count_alert_created() {
        let effect = dlq_count_alert();
        assert!(effect.running());
    }

    #[test]
    fn test_action_success_created() {
        let effect = action_success();
        assert!(effect.running());
    }

    #[test]
    fn test_data_refresh_pulse_created() {
        let effect = data_refresh_pulse();
        assert!(effect.running());
    }
}
