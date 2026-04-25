use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{Frame, layout::Rect};
use std::time::Duration;
use tachyonfx::{Effect, EffectRenderer, Shader};

use crate::client::AppData;
use crate::effects;
use crate::screens;

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Dashboard,
    Patterns,
    Dlq,
    Checkpoints,
    Sagas,
    Config,
}

#[derive(Debug)]
pub enum Action {
    None,
    Quit,
    ApiCall(String, String),
}

pub struct App {
    pub screen: Screen,
    pub data: AppData,
    pub effect_queue: Vec<(Rect, Effect)>,
    // Per-screen state
    pub patterns_selected: usize,
    pub dlq_selected: usize,
    pub dlq_confirm_purge: bool,
    pub checkpoints_selected: usize,
    pub sagas_selected: usize,
    pub sagas_expanded: bool,
    pub config_scroll: u16,
    pub config_validate_result: Option<Result<String, String>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            screen: Screen::Dashboard,
            data: AppData::default(),
            effect_queue: Vec::new(),
            patterns_selected: 0,
            dlq_selected: 0,
            dlq_confirm_purge: false,
            checkpoints_selected: 0,
            sagas_selected: 0,
            sagas_expanded: false,
            config_scroll: 0,
            config_validate_result: None,
        }
    }

    fn screen_idx(&self) -> usize {
        match &self.screen {
            Screen::Dashboard => 0,
            Screen::Patterns => 1,
            Screen::Dlq => 2,
            Screen::Checkpoints => 3,
            Screen::Sagas => 4,
            Screen::Config => 5,
        }
    }

    pub fn switch_screen(&mut self, new_screen: Screen, content_area: Rect) {
        let old_idx = self.screen_idx();
        let new_idx = match &new_screen {
            Screen::Dashboard => 0,
            Screen::Patterns => 1,
            Screen::Dlq => 2,
            Screen::Checkpoints => 3,
            Screen::Sagas => 4,
            Screen::Config => 5,
        };
        let effect = if new_idx > old_idx {
            effects::slide_forward()
        } else {
            effects::slide_back()
        };
        self.effect_queue.push((content_area, effect));
        self.screen = new_screen;
    }

    pub fn push_startup_effect(&mut self) {
        let header = Rect::new(0, 0, 80, 3);
        self.effect_queue.push((header, effects::startup_glitch()));
    }

    pub fn update_data(&mut self, data: AppData) {
        self.data = data;
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Action {
        // Global keys
        if key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c') {
            return Action::Quit;
        }
        match key.code {
            KeyCode::Char('q') => return Action::Quit,
            KeyCode::Char('1') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Dashboard, area);
                return Action::None;
            }
            KeyCode::Char('2') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Patterns, area);
                return Action::None;
            }
            KeyCode::Char('3') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Dlq, area);
                return Action::None;
            }
            KeyCode::Char('4') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Checkpoints, area);
                return Action::None;
            }
            KeyCode::Char('5') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Sagas, area);
                return Action::None;
            }
            KeyCode::Char('6') => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Config, area);
                return Action::None;
            }
            KeyCode::Esc => {
                let area = Rect::new(0, 3, 80, 20);
                self.switch_screen(Screen::Dashboard, area);
                return Action::None;
            }
            _ => {}
        }

        // Screen-specific keys
        match &self.screen {
            Screen::Patterns => self.handle_patterns_key(key),
            Screen::Dlq => self.handle_dlq_key(key),
            Screen::Sagas => self.handle_sagas_key(key),
            Screen::Config => self.handle_config_key(key),
            Screen::Checkpoints => self.handle_checkpoints_key(key),
            Screen::Dashboard => Action::None,
        }
    }

    fn handle_patterns_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Up => {
                if self.patterns_selected > 0 {
                    self.patterns_selected -= 1;
                }
                Action::None
            }
            KeyCode::Down => {
                let max = self.data.patterns.len().saturating_sub(1);
                if self.patterns_selected < max {
                    self.patterns_selected += 1;
                }
                Action::None
            }
            KeyCode::Char('p') => {
                if let Some(p) = self.data.patterns.get(self.patterns_selected) {
                    let path = format!("/patterns/{}/pause", p.name);
                    Action::ApiCall("POST".to_string(), path)
                } else {
                    Action::None
                }
            }
            KeyCode::Char('r') => {
                if let Some(p) = self.data.patterns.get(self.patterns_selected) {
                    let path = format!("/patterns/{}/resume", p.name);
                    Action::ApiCall("POST".to_string(), path)
                } else {
                    Action::None
                }
            }
            KeyCode::Char('x') => {
                if let Some(p) = self.data.patterns.get(self.patterns_selected) {
                    let path = format!("/patterns/{}/replay", p.name);
                    Action::ApiCall("POST".to_string(), path)
                } else {
                    Action::None
                }
            }
            _ => Action::None,
        }
    }

    fn handle_dlq_key(&mut self, key: KeyEvent) -> Action {
        let dlq_topics: Vec<String> = self
            .data
            .patterns
            .iter()
            .map(|p| format!("triad.dlq.{}", p.name))
            .collect();
        let max = dlq_topics.len().saturating_sub(1);

        match key.code {
            KeyCode::Up => {
                if self.dlq_selected > 0 {
                    self.dlq_selected -= 1;
                }
                self.dlq_confirm_purge = false;
                Action::None
            }
            KeyCode::Down => {
                if self.dlq_selected < max {
                    self.dlq_selected += 1;
                }
                self.dlq_confirm_purge = false;
                Action::None
            }
            KeyCode::Char('R') => {
                if let Some(topic) = dlq_topics.get(self.dlq_selected) {
                    let path = format!("/dlq/{}/replay", topic);
                    Action::ApiCall("POST".to_string(), path)
                } else {
                    Action::None
                }
            }
            KeyCode::Char('P') => {
                if !self.dlq_confirm_purge {
                    self.dlq_confirm_purge = true;
                    Action::None
                } else if let Some(topic) = dlq_topics.get(self.dlq_selected) {
                    self.dlq_confirm_purge = false;
                    let path = format!("/dlq/{}", topic);
                    Action::ApiCall("DELETE".to_string(), path)
                } else {
                    Action::None
                }
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                self.dlq_confirm_purge = false;
                Action::None
            }
            _ => Action::None,
        }
    }

    fn handle_sagas_key(&mut self, key: KeyEvent) -> Action {
        let max = self.data.sagas.len().saturating_sub(1);
        match key.code {
            KeyCode::Up => {
                if self.sagas_selected > 0 {
                    self.sagas_selected -= 1;
                    self.sagas_expanded = false;
                }
                Action::None
            }
            KeyCode::Down => {
                if self.sagas_selected < max {
                    self.sagas_selected += 1;
                    self.sagas_expanded = false;
                }
                Action::None
            }
            KeyCode::Enter => {
                self.sagas_expanded = !self.sagas_expanded;
                Action::None
            }
            KeyCode::Char('c') => {
                if let Some(saga) = self.data.sagas.get(self.sagas_selected) {
                    let path = format!("/saga/{}/cancel", saga.saga_id);
                    Action::ApiCall("POST".to_string(), path)
                } else {
                    Action::None
                }
            }
            _ => Action::None,
        }
    }

    fn handle_config_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Up => {
                self.config_scroll = self.config_scroll.saturating_sub(1);
                Action::None
            }
            KeyCode::Down => {
                self.config_scroll += 1;
                Action::None
            }
            KeyCode::Char('v') => {
                let result = triad_core::config::TriadConfig::load("triad.yaml")
                    .map(|_| "Config valid".to_string())
                    .map_err(|e| e.to_string());
                self.config_validate_result = Some(result);
                Action::None
            }
            _ => Action::None,
        }
    }

    fn handle_checkpoints_key(&mut self, key: KeyEvent) -> Action {
        let max = self.data.checkpoints.len().saturating_sub(1);
        match key.code {
            KeyCode::Up => {
                if self.checkpoints_selected > 0 {
                    self.checkpoints_selected -= 1;
                }
                Action::None
            }
            KeyCode::Down => {
                if self.checkpoints_selected < max {
                    self.checkpoints_selected += 1;
                }
                Action::None
            }
            _ => Action::None,
        }
    }

    pub fn render(&mut self, frame: &mut Frame, elapsed: Duration) {
        let last_tick =
            tachyonfx::Duration::from_millis(elapsed.as_millis().min(u128::from(u32::MAX)) as u32);
        let area = frame.area();

        match &self.screen {
            Screen::Dashboard => screens::dashboard::render(frame, area, &self.data),
            Screen::Patterns => {
                screens::patterns::render(frame, area, &self.data, self.patterns_selected)
            }
            Screen::Dlq => screens::dlq::render(
                frame,
                area,
                &self.data,
                self.dlq_selected,
                self.dlq_confirm_purge,
            ),
            Screen::Checkpoints => {
                screens::checkpoints::render(frame, area, &self.data, self.checkpoints_selected)
            }
            Screen::Sagas => screens::sagas::render(
                frame,
                area,
                &self.data,
                self.sagas_selected,
                self.sagas_expanded,
            ),
            Screen::Config => screens::config::render(
                frame,
                area,
                &self.data,
                self.config_scroll,
                &self.config_validate_result,
            ),
        }

        for (effect_area, effect) in &mut self.effect_queue {
            frame.render_effect(effect, *effect_area, last_tick);
        }
        self.effect_queue.retain(|(_, e)| e.running());
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn make_key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn test_app_initial_screen_is_dashboard() {
        let app = App::new();
        assert_eq!(app.screen, Screen::Dashboard);
    }

    #[test]
    fn test_app_default() {
        let app = App::default();
        assert_eq!(app.screen, Screen::Dashboard);
    }

    #[test]
    fn test_screen_switch_via_number_keys() {
        let mut app = App::new();
        app.handle_key(make_key(KeyCode::Char('2')));
        assert_eq!(app.screen, Screen::Patterns);

        app.handle_key(make_key(KeyCode::Char('3')));
        assert_eq!(app.screen, Screen::Dlq);

        app.handle_key(make_key(KeyCode::Char('4')));
        assert_eq!(app.screen, Screen::Checkpoints);

        app.handle_key(make_key(KeyCode::Char('5')));
        assert_eq!(app.screen, Screen::Sagas);

        app.handle_key(make_key(KeyCode::Char('6')));
        assert_eq!(app.screen, Screen::Config);

        app.handle_key(make_key(KeyCode::Char('1')));
        assert_eq!(app.screen, Screen::Dashboard);
    }

    #[test]
    fn test_esc_returns_to_dashboard() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        app.handle_key(make_key(KeyCode::Esc));
        assert_eq!(app.screen, Screen::Dashboard);
    }

    #[test]
    fn test_quit_action_on_q() {
        let mut app = App::new();
        let action = app.handle_key(make_key(KeyCode::Char('q')));
        assert!(matches!(action, Action::Quit));
    }

    #[test]
    fn test_ctrl_c_quit() {
        let mut app = App::new();
        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        let action = app.handle_key(key);
        assert!(matches!(action, Action::Quit));
    }

    #[test]
    fn test_screen_switch_adds_slide_forward_effect() {
        let mut app = App::new();
        app.screen = Screen::Dashboard;
        let key = make_key(KeyCode::Char('2'));
        app.handle_key(key);
        // Switching forward adds a slide_forward effect
        assert!(!app.effect_queue.is_empty());
    }

    #[test]
    fn test_screen_switch_adds_slide_back_effect() {
        let mut app = App::new();
        app.screen = Screen::Config;
        let key = make_key(KeyCode::Char('1'));
        app.handle_key(key);
        // Switching backward adds a slide_back effect
        assert!(!app.effect_queue.is_empty());
    }

    #[test]
    fn test_patterns_navigation() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        app.data.patterns = vec![
            crate::client::PatternInfo {
                name: "outbox".to_string(),
                pattern_type: "outbox".to_string(),
                status: "Running".to_string(),
            },
            crate::client::PatternInfo {
                name: "inbox".to_string(),
                pattern_type: "inbox".to_string(),
                status: "Running".to_string(),
            },
        ];

        assert_eq!(app.patterns_selected, 0);
        app.handle_key(make_key(KeyCode::Down));
        assert_eq!(app.patterns_selected, 1);
        app.handle_key(make_key(KeyCode::Down)); // Can't go past end
        assert_eq!(app.patterns_selected, 1);
        app.handle_key(make_key(KeyCode::Up));
        assert_eq!(app.patterns_selected, 0);
        app.handle_key(make_key(KeyCode::Up)); // Can't go before start
        assert_eq!(app.patterns_selected, 0);
    }

    #[test]
    fn test_patterns_pause_dispatches_api_call() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "outbox".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];
        let action = app.handle_key(make_key(KeyCode::Char('p')));
        assert!(
            matches!(action, Action::ApiCall(ref method, ref path) if method == "POST" && path.contains("pause"))
        );
    }

    #[test]
    fn test_patterns_resume_dispatches_api_call() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "outbox".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Paused".to_string(),
        }];
        let action = app.handle_key(make_key(KeyCode::Char('r')));
        assert!(
            matches!(action, Action::ApiCall(ref method, ref path) if method == "POST" && path.contains("resume"))
        );
    }

    #[test]
    fn test_patterns_replay_dispatches_api_call() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "outbox".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];
        let action = app.handle_key(make_key(KeyCode::Char('x')));
        assert!(
            matches!(action, Action::ApiCall(ref method, ref path) if method == "POST" && path.contains("replay"))
        );
    }

    #[test]
    fn test_dlq_confirm_purge_requires_two_presses() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "orders".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];

        // First P: sets confirm flag
        let action = app.handle_key(make_key(KeyCode::Char('P')));
        assert!(matches!(action, Action::None));
        assert!(app.dlq_confirm_purge);

        // Second P: executes purge
        let action = app.handle_key(make_key(KeyCode::Char('P')));
        assert!(matches!(action, Action::ApiCall(ref method, _) if method == "DELETE"));
        assert!(!app.dlq_confirm_purge);
    }

    #[test]
    fn test_dlq_purge_cancel_with_n() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "orders".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];

        app.handle_key(make_key(KeyCode::Char('P')));
        assert!(app.dlq_confirm_purge);

        app.handle_key(make_key(KeyCode::Char('n')));
        assert!(!app.dlq_confirm_purge);
    }

    #[test]
    fn test_dlq_replay_dispatches_api_call() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "orders".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];
        let action = app.handle_key(make_key(KeyCode::Char('R')));
        assert!(
            matches!(action, Action::ApiCall(ref method, ref path) if method == "POST" && path.contains("replay"))
        );
    }

    #[test]
    fn test_sagas_expand_toggle() {
        let mut app = App::new();
        app.screen = Screen::Sagas;
        assert!(!app.sagas_expanded);
        app.handle_key(make_key(KeyCode::Enter));
        assert!(app.sagas_expanded);
        app.handle_key(make_key(KeyCode::Enter));
        assert!(!app.sagas_expanded);
    }

    #[test]
    fn test_sagas_navigation() {
        let mut app = App::new();
        app.screen = Screen::Sagas;
        app.data.sagas = vec![
            crate::client::SagaInfo {
                saga_id: "saga-1".to_string(),
                saga_name: "order-saga".to_string(),
                current_step: 1,
                status: "Running".to_string(),
            },
            crate::client::SagaInfo {
                saga_id: "saga-2".to_string(),
                saga_name: "payment-saga".to_string(),
                current_step: 2,
                status: "Running".to_string(),
            },
        ];

        assert_eq!(app.sagas_selected, 0);
        app.handle_key(make_key(KeyCode::Down));
        assert_eq!(app.sagas_selected, 1);
        app.handle_key(make_key(KeyCode::Up));
        assert_eq!(app.sagas_selected, 0);
    }

    #[test]
    fn test_sagas_cancel_dispatches_api_call() {
        let mut app = App::new();
        app.screen = Screen::Sagas;
        app.data.sagas = vec![crate::client::SagaInfo {
            saga_id: "saga-abc".to_string(),
            saga_name: "order-saga".to_string(),
            current_step: 1,
            status: "Running".to_string(),
        }];
        let action = app.handle_key(make_key(KeyCode::Char('c')));
        assert!(
            matches!(action, Action::ApiCall(ref method, ref path) if method == "POST" && path.contains("cancel"))
        );
    }

    #[test]
    fn test_checkpoints_navigation() {
        let mut app = App::new();
        app.screen = Screen::Checkpoints;
        app.data.checkpoints = vec![
            crate::client::CheckpointInfo {
                pattern_name: "outbox".to_string(),
                pipeline_name: "main".to_string(),
                owner_instance_id: "i-1".to_string(),
                version: 42,
            },
            crate::client::CheckpointInfo {
                pattern_name: "inbox".to_string(),
                pipeline_name: "secondary".to_string(),
                owner_instance_id: "i-2".to_string(),
                version: 7,
            },
        ];

        assert_eq!(app.checkpoints_selected, 0);
        app.handle_key(make_key(KeyCode::Down));
        assert_eq!(app.checkpoints_selected, 1);
        app.handle_key(make_key(KeyCode::Up));
        assert_eq!(app.checkpoints_selected, 0);
    }

    #[test]
    fn test_config_scroll() {
        let mut app = App::new();
        app.screen = Screen::Config;
        assert_eq!(app.config_scroll, 0);
        app.handle_key(make_key(KeyCode::Down));
        assert_eq!(app.config_scroll, 1);
        app.handle_key(make_key(KeyCode::Up));
        assert_eq!(app.config_scroll, 0);
        app.handle_key(make_key(KeyCode::Up)); // Can't go below 0
        assert_eq!(app.config_scroll, 0);
    }

    #[test]
    fn test_effect_queue_management() {
        let mut app = App::new();
        app.push_startup_effect();
        assert_eq!(app.effect_queue.len(), 1);
    }

    #[test]
    fn test_update_data_replaces() {
        let mut app = App::new();
        assert!(app.data.patterns.is_empty());
        let mut data = crate::client::AppData::default();
        data.patterns = vec![crate::client::PatternInfo {
            name: "test".to_string(),
            pattern_type: "test".to_string(),
            status: "Running".to_string(),
        }];
        app.update_data(data);
        assert_eq!(app.data.patterns.len(), 1);
    }

    #[test]
    fn test_patterns_empty_no_panic_on_key() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        // No patterns — pressing 'p' should return None safely
        let action = app.handle_key(make_key(KeyCode::Char('p')));
        assert!(matches!(action, Action::None));
    }

    #[test]
    fn test_dlq_empty_no_panic_on_purge() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        app.dlq_confirm_purge = true;
        // No patterns/topics — second P should return None safely
        let action = app.handle_key(make_key(KeyCode::Char('P')));
        assert!(matches!(action, Action::None));
    }

    // Render smoke tests — verify each screen does not panic at both common terminal sizes
    fn smoke_render(app: &mut App, width: u16, height: u16) {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| app.render(f, std::time::Duration::from_millis(16)))
            .unwrap();
    }

    #[test]
    fn test_render_dashboard_80x24() {
        let mut app = App::new();
        app.screen = Screen::Dashboard;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_dashboard_220x50() {
        let mut app = App::new();
        app.screen = Screen::Dashboard;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_patterns_80x24() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_patterns_220x50() {
        let mut app = App::new();
        app.screen = Screen::Patterns;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_dlq_80x24() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_dlq_220x50() {
        let mut app = App::new();
        app.screen = Screen::Dlq;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_checkpoints_80x24() {
        let mut app = App::new();
        app.screen = Screen::Checkpoints;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_checkpoints_220x50() {
        let mut app = App::new();
        app.screen = Screen::Checkpoints;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_sagas_80x24() {
        let mut app = App::new();
        app.screen = Screen::Sagas;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_sagas_220x50() {
        let mut app = App::new();
        app.screen = Screen::Sagas;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_config_80x24() {
        let mut app = App::new();
        app.screen = Screen::Config;
        smoke_render(&mut app, 80, 24);
    }

    #[test]
    fn test_render_config_220x50() {
        let mut app = App::new();
        app.screen = Screen::Config;
        smoke_render(&mut app, 220, 50);
    }

    #[test]
    fn test_render_with_data_populated() {
        let mut app = App::new();
        app.data.patterns = vec![crate::client::PatternInfo {
            name: "outbox".to_string(),
            pattern_type: "outbox".to_string(),
            status: "Running".to_string(),
        }];
        app.data.lag = vec![crate::client::LagInfo {
            topic: "orders".to_string(),
            lag_messages: 500,
            pattern_name: "outbox".to_string(),
            partition: 0,
        }];
        app.data.checkpoints = vec![crate::client::CheckpointInfo {
            pattern_name: "outbox".to_string(),
            pipeline_name: "main".to_string(),
            owner_instance_id: "i-1".to_string(),
            version: 1,
        }];
        app.data.sagas = vec![crate::client::SagaInfo {
            saga_id: "s-1".to_string(),
            saga_name: "order-saga".to_string(),
            current_step: 1,
            status: "Running".to_string(),
        }];
        // Render all screens with real data
        for screen in [
            Screen::Dashboard,
            Screen::Patterns,
            Screen::Dlq,
            Screen::Checkpoints,
            Screen::Sagas,
            Screen::Config,
        ] {
            app.screen = screen;
            smoke_render(&mut app, 120, 40);
        }
    }
}
