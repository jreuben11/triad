use anyhow::Result;
use clap::Parser;
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io;
use tokio::sync::mpsc;

mod app;
mod client;
mod effects;
mod screens;
mod widgets;

use app::{Action, App};
use client::AdminClient;

#[derive(Parser)]
#[command(name = "triad-tui", about = "Triad terminal dashboard")]
pub struct Args {
    #[arg(long, env = "TRIAD_ADMIN_URL", default_value = "http://localhost:8080")]
    pub admin_url: String,
    #[arg(long, default_value = "1000")]
    pub poll_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, args).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, args: Args) -> Result<()> {
    let (data_tx, mut data_rx) = mpsc::channel(32);
    let client = AdminClient::new(args.admin_url.clone());
    let poll_ms = args.poll_ms;

    tokio::spawn(async move {
        loop {
            let data = client.fetch_all().await;
            if data_tx.send(data).await.is_err() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
        }
    });

    let mut app = App::new();
    let mut last_tick = std::time::Instant::now();

    app.push_startup_effect();

    loop {
        let elapsed = last_tick.elapsed();
        last_tick = std::time::Instant::now();

        terminal.draw(|frame| {
            app.render(frame, elapsed);
        })?;

        if let Ok(data) = data_rx.try_recv() {
            app.update_data(data);
        }

        if crossterm::event::poll(std::time::Duration::from_millis(50))? {
            if let crossterm::event::Event::Key(key) = crossterm::event::read()? {
                match app.handle_key(key) {
                    Action::Quit => break,
                    Action::ApiCall(method, path) => {
                        let client = AdminClient::new(args.admin_url.clone());
                        tokio::spawn(async move {
                            let _ = client.call(&method, &path).await;
                        });
                    }
                    Action::None => {}
                }
            }
        }
    }

    Ok(())
}
