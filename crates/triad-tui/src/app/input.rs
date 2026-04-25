use crossterm::event::{KeyCode, KeyEvent};

use super::{Action, App};

impl App {
    pub(super) fn handle_patterns_key(&mut self, key: KeyEvent) -> Action {
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

    pub(super) fn handle_dlq_key(&mut self, key: KeyEvent) -> Action {
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

    pub(super) fn handle_sagas_key(&mut self, key: KeyEvent) -> Action {
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

    pub(super) fn handle_config_key(&mut self, key: KeyEvent) -> Action {
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

    pub(super) fn handle_checkpoints_key(&mut self, key: KeyEvent) -> Action {
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
}
