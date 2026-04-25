use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::*;

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
    assert!(!app.effect_queue.is_empty());
}

#[test]
fn test_screen_switch_adds_slide_back_effect() {
    let mut app = App::new();
    app.screen = Screen::Config;
    let key = make_key(KeyCode::Char('1'));
    app.handle_key(key);
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

    let action = app.handle_key(make_key(KeyCode::Char('P')));
    assert!(matches!(action, Action::None));
    assert!(app.dlq_confirm_purge);

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
    let action = app.handle_key(make_key(KeyCode::Char('p')));
    assert!(matches!(action, Action::None));
}

#[test]
fn test_dlq_empty_no_panic_on_purge() {
    let mut app = App::new();
    app.screen = Screen::Dlq;
    app.dlq_confirm_purge = true;
    let action = app.handle_key(make_key(KeyCode::Char('P')));
    assert!(matches!(action, Action::None));
}

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
