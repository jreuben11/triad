use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{Frame, layout::Rect};
use std::time::Duration;
use tachyonfx::{Effect, EffectRenderer, Shader};

use crate::client::AppData;
use crate::effects;
use crate::screens;

mod input;
#[cfg(test)]
mod tests;

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

    pub(super) fn screen_idx(&self) -> usize {
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
